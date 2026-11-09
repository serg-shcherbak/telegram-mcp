"""Bounded metadata journal and transactional watch/outbox state in a separate SQLite DB.

One connection and lock serialize writes; rollback journaling is deliberate. No callback
URL or secret belongs here in plaintext; a later adapter may add encrypted
subscription blobs. Message text, audio, sessions and access hashes are excluded.
An append returning None was refused by a resource ceiling; health records the gap.
"""

from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sqlite3
from threading import RLock
from types import MappingProxyType
from typing import Mapping, Optional, Tuple
from uuid import uuid4

from .models import ScopeTarget, Watch, WatchPolicy, WatchRevision, WatchScope, WatchState
from .scope import ScopeDenied, matches_scope


def _time(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("An aware datetime is required")
    return value.astimezone(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _identifier(value):
    if not isinstance(value, str) or not value or len(value) > 200:
        raise ValueError("A bounded explicit identifier is required")
    return value


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


_BOOL_METADATA = frozenset(
    ("outgoing", "own_send", "sender_bot", "mentioned", "ambiguous", "has_more")
)
_ID_METADATA = frozenset(
    ("sender_id", "reply_to_message_id", "grouped_id", "read_max_id", "unread_count")
)
_METADATA = (
    _BOOL_METADATA
    | _ID_METADATA
    | frozenset(
        (
            "media_type",
            "chat_type",
            "edit_date",
            "deleted_ids",
            "reactions",
            "field_names",
            "message_ids",
        )
    )
)


def _metadata(value):
    if not isinstance(value, Mapping) or set(value) - _METADATA:
        raise ValueError("Only safe event metadata fields are accepted")
    for key, item in value.items():
        valid = False
        if key in _BOOL_METADATA:
            valid = type(item) is bool
        elif key in _ID_METADATA:
            valid = type(item) is int and -(2**63) < item < 2**63
        elif key in ("deleted_ids", "message_ids"):
            valid = (
                isinstance(item, (list, tuple))
                and len(item) <= 100
                and all(type(v) is int and 0 < v < 2**63 for v in item)
            )
        elif key == "media_type":
            valid = item in (
                "photo",
                "video",
                "voice",
                "audio",
                "document",
                "sticker",
                "animation",
                "contact",
                "location",
                "poll",
                "web_page",
                "unsupported",
                None,
            )
        elif key == "chat_type":
            valid = item in ("private", "group", "supergroup", "channel")
        elif key == "edit_date":
            try:
                valid = isinstance(item, str) and _time(datetime.fromisoformat(item)) is not None
            except ValueError:
                pass
        elif key == "field_names":
            valid = (
                isinstance(item, (list, tuple))
                and len(item) <= 20
                and all(
                    v
                    in (
                        "title",
                        "photo",
                        "username",
                        "about",
                        "permissions",
                        "pinned",
                        "archived",
                        "muted",
                        "closed",
                        "hidden",
                        "unread",
                        "membership",
                        "access",
                    )
                    for v in item
                )
            )
        elif key == "reactions":
            valid = (
                isinstance(item, (list, tuple))
                and len(item) <= 32
                and all(
                    isinstance(v, (list, tuple))
                    and len(v) == 2
                    and isinstance(v[0], str)
                    and len(v[0]) <= 16
                    and type(v[1]) is int
                    and 0 <= v[1] < 2**63
                    for v in item
                )
            )
        if not valid:
            raise ValueError("Event metadata does not match its bounded field schema")
    return _freeze(_plain(value))


@dataclass(frozen=True)
class StoreLimits:
    max_watches: int = 20
    max_chats: int = 50
    max_topics: int = 100
    max_sources: int = 10_000
    max_payload_bytes: int = 32 * 1024 * 1024
    max_batches: int = 10_000
    max_deliveries: int = 20_000
    max_obligations: int = 10_000
    max_checkpoint_keys: int = 200
    max_budget_keys: int = 1000
    min_free_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in asdict(self).values()):
            raise ValueError("Store ceilings must be positive integers")


@dataclass(frozen=True)
class SourceRecord:
    target: ScopeTarget
    kind: str
    message_id: Optional[int]
    occurred_at: datetime
    version: str = "1"
    metadata: Mapping = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.target, ScopeTarget):
            raise ValueError("Resolved source target required")
        _identifier(self.kind)
        _identifier(self.version)
        _time(self.occurred_at)
        if self.message_id is not None and (
            type(self.message_id) is not int or not 0 < self.message_id < 2**63
        ):
            raise ValueError("Positive message ID required")
        object.__setattr__(self, "metadata", _metadata(self.metadata))


@dataclass(frozen=True)
class StoredSource:
    sequence: int
    source_id: str
    event_id: str
    watch_id: str
    epoch: int
    record: SourceRecord


@dataclass(frozen=True)
class DeliveryRequest:
    recipient_id: str
    subscription_id: str

    def __post_init__(self):
        _identifier(self.recipient_id)
        _identifier(self.subscription_id)


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    event_id: str
    watch_id: str
    epoch: int
    recipient_id: str
    subscription_id: str
    payload: Mapping
    state: str
    attempts: int
    due_at: datetime
    claim_token: Optional[str]


@dataclass(frozen=True)
class Health:
    status: str
    reason: str
    gap_count: int
    retention_floor: int


@dataclass(frozen=True)
class AnalysisSnapshot:
    owner_id: str
    watch_id: str
    epoch: int
    watermark: int
    obligation_versions: Tuple[Tuple[str, int], ...]


@dataclass(frozen=True)
class ObligationChange:
    obligation_id: str
    state: str
    summary: str
    evidence: Tuple[int, ...]

    def __post_init__(self):
        _identifier(self.obligation_id)
        if self.state not in (
            "needs_review",
            "awaiting_me",
            "awaiting_them",
            "done",
            "snoozed",
            "cancelled",
        ):
            raise ValueError("Invalid obligation state")
        if not isinstance(self.summary, str) or len(self.summary) > 500:
            raise ValueError("A conclusion is limited to 500 characters")
        object.__setattr__(self, "evidence", tuple(self.evidence))
        if len(self.evidence) > 100 or any(type(v) is not int or v <= 0 for v in self.evidence):
            raise ValueError("Bounded evidence sequence references required")


@dataclass(frozen=True)
class Obligation:
    obligation_id: str
    version: int
    state: str
    summary: str
    evidence: Tuple[int, ...]


class StoreUnavailable(RuntimeError):
    """Storage failed; processing must stop rather than treating this as no events."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS watches (
 owner TEXT NOT NULL, watch TEXT NOT NULL, version INTEGER NOT NULL, epoch INTEGER NOT NULL,
 document TEXT NOT NULL, watermark INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(owner,watch));
CREATE TABLE IF NOT EXISTS source_events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, watch TEXT NOT NULL,
 epoch INTEGER NOT NULL, source_id TEXT NOT NULL, event_id TEXT NOT NULL,
 occurred TEXT NOT NULL, document TEXT NOT NULL,
 UNIQUE(owner,watch,epoch,source_id),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS source_checkpoints (
 owner TEXT NOT NULL, watch TEXT NOT NULL, target TEXT NOT NULL, stream TEXT NOT NULL,
 epoch INTEGER NOT NULL, position TEXT NOT NULL, PRIMARY KEY(owner,watch,target,stream),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS delivery_batches (
 owner TEXT NOT NULL, watch TEXT NOT NULL, epoch INTEGER NOT NULL, event_id TEXT NOT NULL,
 created TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(owner,event_id),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS deliveries (
 delivery_id TEXT PRIMARY KEY, owner TEXT NOT NULL, event_id TEXT NOT NULL,
 recipient TEXT NOT NULL, subscription TEXT NOT NULL, state TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0, due TEXT NOT NULL, lease_until TEXT,
 token TEXT, UNIQUE(owner,recipient,subscription,event_id),
 FOREIGN KEY(owner,event_id) REFERENCES delivery_batches(owner,event_id) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(owner,state,due);
CREATE TABLE IF NOT EXISTS budget_buckets (
 owner TEXT NOT NULL, watch TEXT NOT NULL, name TEXT NOT NULL, bucket TEXT NOT NULL,
 used INTEGER NOT NULL, PRIMARY KEY(owner,watch,name,bucket),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS obligations (
 owner TEXT NOT NULL, watch TEXT NOT NULL, obligation TEXT NOT NULL,
 version INTEGER NOT NULL, document TEXT NOT NULL, PRIMARY KEY(owner,watch,obligation),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS health (
 owner TEXT NOT NULL, watch TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'healthy',
 reason TEXT NOT NULL DEFAULT '', gaps INTEGER NOT NULL DEFAULT 0,
 retention_floor INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(owner,watch),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
PRAGMA user_version=1;
"""


def _watch_document(watch):
    document = asdict(watch)
    document["state"] = watch.state.value
    document["boundary"] = _time(watch.boundary) if watch.boundary else None
    return _json(document)


def _watch(document):
    data = json.loads(document)
    for name in ("active", "draft"):
        item = data[name]
        if item is not None:
            scope = item["scope"]
            item["scope"] = WatchScope(
                tuple(ScopeTarget(**v) for v in scope["targets"]), scope["folder_id"]
            )
            item["policy"] = WatchPolicy(**item["policy"])
            data[name] = WatchRevision(**item)
    data["state"] = WatchState(data["state"])
    data["boundary"] = datetime.fromisoformat(data["boundary"]) if data["boundary"] else None
    return Watch(**data)


class SQLiteStore:
    def __init__(self, path, *, limits=StoreLimits()):
        self.path = Path(path)
        self.limits = limits
        self._lock = RLock()
        self._volatile = {}
        self._db = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA journal_mode=DELETE")
        self._db.execute("PRAGMA synchronous=FULL")
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            self._db.close()
            raise ValueError("Unsupported event database schema")
        self._db.executescript(_SCHEMA)

    @property
    def journal_mode(self):
        with self._lock:
            return self._db.execute("PRAGMA journal_mode").fetchone()[0]

    def close(self):
        with self._lock:
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @contextmanager
    def _transaction(self):
        with self._lock:
            try:
                self._db.execute("BEGIN IMMEDIATE")
                yield self._db
                self._db.commit()
            except sqlite3.Error as exc:
                self._db.rollback()
                raise StoreUnavailable(
                    "Event storage failed; coverage requires reconciliation"
                ) from exc
            except BaseException:
                self._db.rollback()
                raise

    def _get(self, db, owner, watch):
        row = db.execute(
            "SELECT document FROM watches WHERE owner=? AND watch=?", (owner, watch)
        ).fetchone()
        return _watch(row[0]) if row else None

    def get(self, owner_id, watch_id):
        with self._transaction() as db:
            return self._get(db, owner_id, watch_id)

    def list(self, owner_id):
        with self._transaction() as db:
            return tuple(
                _watch(row[0])
                for row in db.execute(
                    "SELECT document FROM watches WHERE owner=? ORDER BY watch", (owner_id,)
                )
            )

    def compare_and_swap(self, owner_id, watch_id, expected_version, replacement):
        if replacement is not None:
            if replacement.owner_id != owner_id or replacement.watch_id != watch_id:
                return False
            document = _watch_document(replacement)
            if len(document.encode()) > 64 * 1024:
                raise ValueError("Watch metadata exceeds its bounded envelope")
        if expected_version is not None and type(expected_version) is not int:
            return False
        with self._transaction() as db:
            current = self._get(db, owner_id, watch_id)
            if (current.version if current else None) != expected_version:
                return False
            if replacement is None:
                if current is not None and current.state != WatchState.REVOKED:
                    raise ScopeDenied("Revoke the watch before deleting its data")
                db.execute("DELETE FROM watches WHERE owner=? AND watch=?", (owner_id, watch_id))
                return True
            if (
                current is None
                and db.execute(
                    "SELECT COUNT(*) FROM watches WHERE owner=?", (owner_id,)
                ).fetchone()[0]
                >= self.limits.max_watches
            ):
                raise ValueError("Watch resource ceiling reached")
            active = [w for w in self._all(db, owner_id) if w.watch_id != watch_id]
            active.append(replacement)
            targets = {
                t
                for w in active
                if w.state == WatchState.ACTIVE and w.active
                for t in w.active.scope.targets
            }
            if (
                len({(t.account_id, t.marked_chat_id) for t in targets}) > self.limits.max_chats
                or sum(t.topic_id is not None for t in targets) > self.limits.max_topics
            ):
                raise ValueError("Selected scope resource ceiling reached")
            db.execute(
                "INSERT INTO watches(owner,watch,version,epoch,document) VALUES(?,?,?,?,?) ON CONFLICT(owner,watch) DO UPDATE SET version=excluded.version,epoch=excluded.epoch,document=excluded.document",
                (owner_id, watch_id, replacement.version, replacement.epoch, document),
            )
            db.execute(
                "INSERT OR IGNORE INTO health(owner,watch) VALUES(?,?)", (owner_id, watch_id)
            )
            if current is not None and current.epoch != replacement.epoch:
                db.execute(
                    "UPDATE deliveries SET state='cancelled',token=NULL,lease_until=NULL WHERE owner=? AND state IN ('pending','sending') AND event_id IN (SELECT event_id FROM delivery_batches WHERE owner=? AND watch=? AND epoch!=?)",
                    (owner_id, owner_id, watch_id, replacement.epoch),
                )
                db.execute(
                    "DELETE FROM source_checkpoints WHERE owner=? AND watch=?",
                    (owner_id, watch_id),
                )
            return True

    def _all(self, db, owner):
        return tuple(
            _watch(row[0])
            for row in db.execute("SELECT document FROM watches WHERE owner=?", (owner,))
        )

    def _active(self, db, owner, watch, epoch, target=None):
        value = self._get(db, owner, watch)
        if (
            type(epoch) is not int
            or value is None
            or value.state != WatchState.ACTIVE
            or value.epoch != epoch
            or value.active is None
            or (target is not None and not matches_scope(value.active.scope, target))
        ):
            raise ScopeDenied("Current active watch scope and epoch required")
        return value

    def _gap(self, db, owner, watch, reason):
        db.execute(
            "UPDATE health SET status='degraded',reason=?,gaps=gaps+1 WHERE owner=? AND watch=?",
            (reason, owner, watch),
        )

    def health(self, owner_id, watch_id):
        with self._transaction() as db:
            row = db.execute(
                "SELECT status,reason,gaps,retention_floor FROM health WHERE owner=? AND watch=?",
                (owner_id, watch_id),
            ).fetchone()
            return self._volatile.get((owner_id, watch_id), Health(*row)) if row else None

    def mark_gap(self, owner_id, watch_id, epoch, reason="coverage_gap"):
        if reason not in (
            "coverage_gap",
            "disconnected",
            "ambiguous_source",
            "telegram_limit",
            "storage_limit",
            "reconciliation_required",
        ):
            raise ValueError("Use a safe coverage reason")
        with self._transaction() as db:
            self._active(db, owner_id, watch_id, epoch)
            self._gap(db, owner_id, watch_id, reason)

    def _capacity(
        self, db, owner, watch, *, sources=0, batches=0, deliveries=0, size=0, obligations=0
    ):
        for table, increment, ceiling in (
            ("source_events", sources, self.limits.max_sources),
            ("delivery_batches", batches, self.limits.max_batches),
            ("deliveries", deliveries, self.limits.max_deliveries),
            ("obligations", obligations, self.limits.max_obligations),
        ):
            if (
                increment
                and db.execute(f"SELECT COUNT(*) FROM {table} WHERE owner=?", (owner,)).fetchone()[
                    0
                ]
                + increment
                > ceiling
            ):
                self._gap(db, owner, watch, "storage_limit")
                return False
        total = sum(
            db.execute(
                f"SELECT COALESCE(SUM(length(CAST(document AS BLOB))),0) FROM {table} WHERE owner=?",
                (owner,),
            ).fetchone()[0]
            for table in ("source_events", "delivery_batches", "obligations")
        )
        if (
            total + size > self.limits.max_payload_bytes
            or shutil.disk_usage(self.path.parent).free < self.limits.min_free_bytes
        ):
            self._gap(db, owner, watch, "storage_limit")
            return False
        return True

    def _source(self, row):
        data = json.loads(row["document"])
        data["target"] = ScopeTarget(**data["target"])
        data["occurred_at"] = datetime.fromisoformat(data["occurred_at"])
        return StoredSource(
            row["seq"],
            row["source_id"],
            row["event_id"],
            row["watch"],
            row["epoch"],
            SourceRecord(**data),
        )

    def append_source(self, owner_id, watch_id, epoch, record, *, deliveries=()):
        if not isinstance(record, SourceRecord) or any(
            not isinstance(r, DeliveryRequest) for r in deliveries
        ):
            raise ValueError("Validated source and delivery contracts required")
        document = {
            "target": asdict(record.target),
            "kind": record.kind,
            "message_id": record.message_id,
            "occurred_at": _time(record.occurred_at),
            "version": record.version,
            "metadata": _plain(record.metadata),
        }
        encoded = _json(document)
        identity = {k: v for k, v in document.items() if k not in ("metadata", "occurred_at")}
        source_id = sha256(_json(identity).encode()).hexdigest()
        event_id = sha256(_json((owner_id, watch_id, epoch, source_id)).encode()).hexdigest()
        try:
            with self._transaction() as db:
                watch = self._active(db, owner_id, watch_id, epoch, record.target)
                if deliveries and not watch.active.policy.wake_dot:
                    raise ScopeDenied("Dot delivery is not enabled for this watch")
                # Telegram dates have second precision. The activation second is
                # included; timestamps cannot distinguish earlier work within it.
                boundary = watch.boundary.replace(microsecond=0) if watch.boundary else None
                if record.kind not in watch.active.capabilities or (
                    boundary and record.occurred_at < boundary
                ):
                    raise ScopeDenied("Source predates activation or lacks an active capability")
                existing = db.execute(
                    "SELECT * FROM source_events WHERE owner=? AND watch=? AND epoch=? AND source_id=?",
                    (owner_id, watch_id, epoch, source_id),
                ).fetchone()
                if existing:
                    return self._source(existing)
                previous_sequence = db.execute(
                    "SELECT seq FROM sqlite_sequence WHERE name='source_events'"
                ).fetchone()
                next_sequence = (previous_sequence[0] if previous_sequence else 0) + 1
                payload = {
                    "source_ids": [source_id],
                    "sequences": [next_sequence],
                    "has_more": False,
                }
                delivery_count = len(set(deliveries))
                if not self._capacity(
                    db,
                    owner_id,
                    watch_id,
                    sources=1,
                    batches=bool(deliveries),
                    deliveries=delivery_count,
                    size=len(encoded.encode())
                    + (len(_json(payload).encode()) if deliveries else 0),
                ):
                    return None
                cur = db.execute(
                    "INSERT INTO source_events(owner,watch,epoch,source_id,event_id,occurred,document) VALUES(?,?,?,?,?,?,?)",
                    (
                        owner_id,
                        watch_id,
                        epoch,
                        source_id,
                        event_id,
                        _time(record.occurred_at),
                        encoded,
                    ),
                )
                sequence = cur.lastrowid
                db.execute(
                    "UPDATE watches SET watermark=? WHERE owner=? AND watch=?",
                    (sequence, owner_id, watch_id),
                )
                if deliveries:
                    payload["sequences"] = [sequence]
                    self._enqueue(
                        db,
                        owner_id,
                        watch_id,
                        epoch,
                        event_id,
                        payload,
                        deliveries,
                        record.occurred_at,
                    )
                return StoredSource(sequence, source_id, event_id, watch_id, epoch, record)
        except StoreUnavailable:
            self._volatile[(owner_id, watch_id)] = Health("degraded", "storage_failure", 1, 0)
            # The failed content transaction was rolled back. Preserve the gap in
            # a separate small control transaction when the disk still permits it.
            try:
                with self._transaction() as db:
                    self._gap(db, owner_id, watch_id, "storage_failure")
                self._volatile.pop((owner_id, watch_id), None)
            except StoreUnavailable:
                pass
            raise

    def get_sources(self, owner_id, watch_id, *, after=0, limit=1000):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Source page size must be between 1 and 1000")
        with self._transaction() as db:
            return tuple(
                self._source(row)
                for row in db.execute(
                    "SELECT * FROM source_events WHERE owner=? AND watch=? AND seq>? ORDER BY seq LIMIT ?",
                    (owner_id, watch_id, after, limit),
                )
            )

    def get_checkpoint(self, owner_id, watch_id, target, stream):
        with self._transaction() as db:
            row = db.execute(
                "SELECT position FROM source_checkpoints WHERE owner=? AND watch=? AND target=? AND stream=?",
                (owner_id, watch_id, _json(asdict(target)), stream),
            ).fetchone()
            return row[0] if row else None

    def set_checkpoint(self, owner_id, watch_id, epoch, target, stream, position):
        _identifier(stream)
        _identifier(position)
        with self._transaction() as db:
            self._active(db, owner_id, watch_id, epoch, target)
            key = (owner_id, watch_id, _json(asdict(target)), stream)
            if (
                not db.execute(
                    "SELECT 1 FROM source_checkpoints WHERE owner=? AND watch=? AND target=? AND stream=?",
                    key,
                ).fetchone()
                and db.execute(
                    "SELECT COUNT(*) FROM source_checkpoints WHERE owner=?", (owner_id,)
                ).fetchone()[0]
                >= self.limits.max_checkpoint_keys
            ):
                self._gap(db, owner_id, watch_id, "storage_limit")
                return False
            db.execute(
                "INSERT INTO source_checkpoints(owner,watch,target,stream,epoch,position) VALUES(?,?,?,?,?,?) ON CONFLICT(owner,watch,target,stream) DO UPDATE SET epoch=excluded.epoch,position=excluded.position",
                (*key, epoch, position),
            )
            return True

    def _enqueue(self, db, owner, watch, epoch, event_id, payload, requests, now):
        db.execute(
            "INSERT OR IGNORE INTO delivery_batches(owner,watch,epoch,event_id,created,document) VALUES(?,?,?,?,?,?)",
            (owner, watch, epoch, event_id, _time(now), _json(payload)),
        )
        for request in set(requests):
            delivery_id = sha256(
                _json((owner, request.recipient_id, request.subscription_id, event_id)).encode()
            ).hexdigest()
            db.execute(
                "INSERT OR IGNORE INTO deliveries(delivery_id,owner,event_id,recipient,subscription,state,due) VALUES(?,?,?,?,?,'pending',?)",
                (
                    delivery_id,
                    owner,
                    event_id,
                    request.recipient_id,
                    request.subscription_id,
                    _time(now),
                ),
            )

    def enqueue_batch(
        self, owner_id, watch_id, epoch, source_sequences, *, deliveries, now, has_more=False
    ):
        sequences = tuple(sorted(set(source_sequences)))
        if (
            not sequences
            or len(sequences) > 100
            or any(type(v) is not int or v <= 0 for v in sequences)
            or any(not isinstance(r, DeliveryRequest) for r in deliveries)
            or type(has_more) is not bool
        ):
            raise ValueError("A bounded source batch and delivery requests are required")
        with self._transaction() as db:
            watch = self._active(db, owner_id, watch_id, epoch)
            if deliveries and not watch.active.policy.wake_dot:
                raise ScopeDenied("Dot delivery is not enabled for this watch")
            rows = db.execute(
                "SELECT seq,source_id FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq IN ("
                + ",".join("?" for _ in sequences)
                + ") ORDER BY seq",
                (owner_id, watch_id, epoch, *sequences),
            ).fetchall()
            if len(rows) != len(sequences):
                raise ScopeDenied("Every batch source must belong to the current watch epoch")
            payload = {
                "source_ids": [r["source_id"] for r in rows],
                "sequences": list(sequences),
                "has_more": has_more,
            }
            event_id = sha256(
                _json((owner_id, watch_id, epoch, sequences, has_more)).encode()
            ).hexdigest()
            if db.execute(
                "SELECT 1 FROM delivery_batches WHERE owner=? AND event_id=?",
                (owner_id, event_id),
            ).fetchone():
                missing = sum(
                    not db.execute(
                        "SELECT 1 FROM deliveries WHERE owner=? AND recipient=? AND subscription=? AND event_id=?",
                        (owner_id, request.recipient_id, request.subscription_id, event_id),
                    ).fetchone()
                    for request in set(deliveries)
                )
                if not self._capacity(db, owner_id, watch_id, deliveries=missing):
                    return None
                self._enqueue(db, owner_id, watch_id, epoch, event_id, payload, deliveries, now)
                return event_id
            if not self._capacity(
                db,
                owner_id,
                watch_id,
                batches=1,
                deliveries=len(set(deliveries)),
                size=len(_json(payload).encode()),
            ):
                return None
            self._enqueue(db, owner_id, watch_id, epoch, event_id, payload, deliveries, now)
            return event_id

    def _delivery(self, row):
        return Delivery(
            row["delivery_id"],
            row["event_id"],
            row["watch"],
            row["epoch"],
            row["recipient"],
            row["subscription"],
            _freeze(json.loads(row["document"])),
            row["state"],
            row["attempts"],
            datetime.fromisoformat(row["due"]),
            row["token"],
        )

    def _delivery_row(self, db, owner, delivery_id):
        return db.execute(
            "SELECT d.*,b.watch,b.epoch,b.document FROM deliveries d JOIN delivery_batches b ON d.owner=b.owner AND d.event_id=b.event_id WHERE d.owner=? AND d.delivery_id=?",
            (owner, delivery_id),
        ).fetchone()

    def get_delivery(self, owner_id, delivery_id):
        with self._transaction() as db:
            row = self._delivery_row(db, owner_id, delivery_id)
            return self._delivery(row) if row else None

    def claim_due(self, owner_id, *, now, limit=1, lease_seconds=60):
        from datetime import timedelta

        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or type(lease_seconds) is not int
            or lease_seconds <= 0
        ):
            raise ValueError("Bounded positive delivery lease required")
        with self._transaction() as db:
            rows = db.execute(
                "SELECT d.*,b.watch,b.epoch,b.document FROM deliveries d JOIN delivery_batches b ON d.owner=b.owner AND d.event_id=b.event_id WHERE d.owner=? AND ((d.state='pending' AND d.due<=?) OR (d.state='sending' AND d.lease_until<=?)) ORDER BY d.due,d.delivery_id LIMIT ?",
                (owner_id, _time(now), _time(now), limit),
            ).fetchall()
            claimed = []
            for row in rows:
                try:
                    self._active(db, owner_id, row["watch"], row["epoch"])
                except ScopeDenied:
                    db.execute(
                        "UPDATE deliveries SET state='cancelled',token=NULL WHERE delivery_id=?",
                        (row["delivery_id"],),
                    )
                    continue
                db.execute(
                    "UPDATE deliveries SET state='sending',attempts=attempts+1,token=?,lease_until=? WHERE delivery_id=?",
                    (
                        str(uuid4()),
                        _time(now + timedelta(seconds=lease_seconds)),
                        row["delivery_id"],
                    ),
                )
                claimed.append(
                    self._delivery(self._delivery_row(db, owner_id, row["delivery_id"]))
                )
            return tuple(claimed)

    def _current_delivery(self, db, owner, delivery_id, token):
        row = self._delivery_row(db, owner, delivery_id)
        if row is None or row["state"] != "sending" or not token or row["token"] != token:
            return False
        try:
            self._active(db, owner, row["watch"], row["epoch"])
        except ScopeDenied:
            return False
        return True

    def delivery_current(self, owner_id, delivery_id, claim_token):
        """Final local epoch check before network I/O; an in-flight request cannot be recalled."""
        with self._transaction() as db:
            return self._current_delivery(db, owner_id, delivery_id, claim_token)

    def ack(self, owner_id, delivery_id, claim_token):
        with self._transaction() as db:
            if not self._current_delivery(db, owner_id, delivery_id, claim_token):
                return False
            db.execute(
                "UPDATE deliveries SET state='acked',token=NULL,lease_until=NULL WHERE owner=? AND delivery_id=?",
                (owner_id, delivery_id),
            )
            return True

    def retry(self, owner_id, delivery_id, claim_token, *, due_at, dead=False):
        with self._transaction() as db:
            if not self._current_delivery(db, owner_id, delivery_id, claim_token):
                return False
            db.execute(
                "UPDATE deliveries SET state=?,due=?,token=NULL,lease_until=NULL WHERE owner=? AND delivery_id=?",
                ("dead" if dead else "pending", _time(due_at), owner_id, delivery_id),
            )
            return True

    def reserve_budget(self, owner_id, watch_id, epoch, name, bucket, amount, *, limit):
        _identifier(name)
        if type(amount) is not int or amount <= 0 or type(limit) is not int or limit < 0:
            raise ValueError("Positive budget reservation required")
        with self._transaction() as db:
            self._active(db, owner_id, watch_id, epoch)
            key = (owner_id, watch_id, name, _time(bucket))
            row = db.execute(
                "SELECT used FROM budget_buckets WHERE owner=? AND watch=? AND name=? AND bucket=?",
                key,
            ).fetchone()
            if (row[0] if row else 0) + amount > limit:
                return False
            if (
                row is None
                and db.execute(
                    "SELECT COUNT(*) FROM budget_buckets WHERE owner=?", (owner_id,)
                ).fetchone()[0]
                >= self.limits.max_budget_keys
            ):
                self._gap(db, owner_id, watch_id, "storage_limit")
                return False
            db.execute(
                "INSERT INTO budget_buckets(owner,watch,name,bucket,used) VALUES(?,?,?,?,?) ON CONFLICT(owner,watch,name,bucket) DO UPDATE SET used=used+excluded.used",
                (*key, amount),
            )
            return True

    def analysis_snapshot(self, owner_id, watch_id, epoch, obligation_ids=()):
        ids = tuple(sorted(set(obligation_ids)))
        if len(ids) > 100:
            raise ValueError("Too many obligation versions")
        for value in ids:
            _identifier(value)
        with self._transaction() as db:
            self._active(db, owner_id, watch_id, epoch)
            watermark = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?", (owner_id, watch_id)
            ).fetchone()[0]
            versions = []
            for value in ids:
                row = db.execute(
                    "SELECT version FROM obligations WHERE owner=? AND watch=? AND obligation=?",
                    (owner_id, watch_id, value),
                ).fetchone()
                versions.append((value, row[0] if row else 0))
            return AnalysisSnapshot(owner_id, watch_id, epoch, watermark, tuple(versions))

    def commit_analysis(self, snapshot, changes):
        if not isinstance(snapshot, AnalysisSnapshot) or any(
            not isinstance(c, ObligationChange) for c in changes
        ):
            raise ValueError("Validated analysis snapshot and conclusions required")
        expected = dict(snapshot.obligation_versions)
        if len({c.obligation_id for c in changes}) != len(changes) or any(
            c.obligation_id not in expected for c in changes
        ):
            raise ValueError("All changed obligations must be pinned once in the snapshot")
        with self._transaction() as db:
            try:
                watch = self._active(db, snapshot.owner_id, snapshot.watch_id, snapshot.epoch)
            except ScopeDenied:
                return False
            if not watch.active.policy.store_state:
                raise ScopeDenied("State persistence is not enabled")
            watermark = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?",
                (snapshot.owner_id, snapshot.watch_id),
            ).fetchone()[0]
            if watermark != snapshot.watermark:
                return False
            for obligation, version in expected.items():
                row = db.execute(
                    "SELECT version FROM obligations WHERE owner=? AND watch=? AND obligation=?",
                    (snapshot.owner_id, snapshot.watch_id, obligation),
                ).fetchone()
                if (row[0] if row else 0) != version:
                    return False
            for change in changes:
                for seq in change.evidence:
                    if not db.execute(
                        "SELECT 1 FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq=?",
                        (snapshot.owner_id, snapshot.watch_id, snapshot.epoch, seq),
                    ).fetchone():
                        raise ScopeDenied("Evidence must belong to this active watch epoch")
            documents = [_json(asdict(c)) for c in changes]
            if not self._capacity(
                db,
                snapshot.owner_id,
                snapshot.watch_id,
                obligations=sum(expected[c.obligation_id] == 0 for c in changes),
                size=sum(len(v.encode()) for v in documents),
            ):
                return False
            for change, document in zip(changes, documents):
                db.execute(
                    "INSERT INTO obligations(owner,watch,obligation,version,document) VALUES(?,?,?,?,?) ON CONFLICT(owner,watch,obligation) DO UPDATE SET version=excluded.version,document=excluded.document",
                    (
                        snapshot.owner_id,
                        snapshot.watch_id,
                        change.obligation_id,
                        expected[change.obligation_id] + 1,
                        document,
                    ),
                )
            return True

    def get_obligations(self, owner_id, watch_id):
        with self._transaction() as db:
            return tuple(
                Obligation(
                    row["obligation"],
                    row["version"],
                    data["state"],
                    data["summary"],
                    tuple(data["evidence"]),
                )
                for row in db.execute(
                    "SELECT * FROM obligations WHERE owner=? AND watch=? ORDER BY obligation",
                    (owner_id, watch_id),
                )
                for data in (json.loads(row["document"]),)
            )

    def prune(self, owner_id, *, before):
        """Expire finite replay, cancel dependent delivery work, and expose its floor.

        Operators must expire backup copies on the same schedule. This database has
        no excerpts; active conclusions survive until explicit watch/item deletion.
        """
        boundary = _time(before)
        with self._transaction() as db:
            rows = db.execute(
                "SELECT watch,MAX(seq) AS floor,COUNT(*) AS count FROM source_events WHERE owner=? AND occurred<? GROUP BY watch",
                (owner_id, boundary),
            ).fetchall()
            for row in rows:
                db.execute(
                    "UPDATE health SET retention_floor=MAX(retention_floor,?) WHERE owner=? AND watch=?",
                    (row["floor"], owner_id, row["watch"]),
                )
            for row in rows:
                self._gap(db, owner_id, row["watch"], "retention_expired")
            db.execute(
                "DELETE FROM delivery_batches WHERE owner=? AND created<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM source_events WHERE owner=? AND occurred<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM budget_buckets WHERE owner=? AND bucket<?", (owner_id, boundary)
            )
            return sum(row["count"] for row in rows)
