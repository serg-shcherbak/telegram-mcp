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
    confidence: float = 1.0
    due_at: Optional[str] = None
    due_origin: Optional[str] = None

    def __post_init__(self):
        _identifier(self.obligation_id)
        if type(self.confidence) not in (int, float) or not 0 <= self.confidence <= 1:
            raise ValueError("Confidence must be in 0..1")
        if self.confidence < 0.8:
            object.__setattr__(self, "state", "needs_review")
            object.__setattr__(self, "due_at", None)
            object.__setattr__(self, "due_origin", None)
        if self.due_at is not None:
            _time(datetime.fromisoformat(self.due_at))
            if self.due_origin not in ("user", "message"):
                raise ValueError("Explicit deadline provenance required")
        elif self.due_origin is not None:
            raise ValueError("Deadline provenance needs a deadline")
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
    confidence: float = 1.0
    due_at: Optional[str] = None
    due_origin: Optional[str] = None

    @property
    def expected_actor(self):
        return {"awaiting_me": "me", "awaiting_them": "them"}.get(self.state)


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
CREATE TABLE IF NOT EXISTS policy_global_budgets (
 owner TEXT NOT NULL, bucket TEXT NOT NULL, used INTEGER NOT NULL,
 PRIMARY KEY(owner,bucket));
CREATE TABLE IF NOT EXISTS policy_progress (
 owner TEXT NOT NULL, watch TEXT NOT NULL, epoch INTEGER NOT NULL,
 cursor INTEGER NOT NULL DEFAULT 0, last_wake TEXT,
 PRIMARY KEY(owner,watch), FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS recipient_sources (
 owner TEXT NOT NULL, watch TEXT NOT NULL, recipient TEXT NOT NULL, source_id TEXT NOT NULL, created TEXT NOT NULL,
 PRIMARY KEY(owner,recipient,source_id),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS reminder_rules (
 owner TEXT NOT NULL, watch TEXT NOT NULL, obligation TEXT NOT NULL, epoch INTEGER NOT NULL,
 version INTEGER NOT NULL, watermark INTEGER NOT NULL, due TEXT NOT NULL, fired INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(owner,watch,obligation), FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS event_subscriptions (
 owner TEXT NOT NULL, identity TEXT NOT NULL, subscription TEXT NOT NULL, watch TEXT NOT NULL,
 epoch INTEGER NOT NULL, expires TEXT NOT NULL, document BLOB NOT NULL,
 PRIMARY KEY(owner,identity), UNIQUE(owner,subscription),
 FOREIGN KEY(owner,watch) REFERENCES watches(owner,watch) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS event_replays (
 owner TEXT NOT NULL, subscription TEXT NOT NULL, event_id TEXT NOT NULL,
 PRIMARY KEY(owner,subscription,event_id),
 FOREIGN KEY(owner,event_id) REFERENCES delivery_batches(owner,event_id) ON DELETE CASCADE);
CREATE TABLE IF NOT EXISTS event_frontiers (
 owner TEXT NOT NULL, subscription TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0,
 floor INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(owner,subscription));
CREATE TABLE IF NOT EXISTS event_history (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, owner TEXT NOT NULL, event_id TEXT NOT NULL,
 UNIQUE(owner,event_id), FOREIGN KEY(owner,event_id) REFERENCES delivery_batches(owner,event_id) ON DELETE CASCADE);
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
                stale = db.execute(
                    "SELECT subscription FROM event_subscriptions WHERE owner=? AND watch=? AND epoch!=?",
                    (owner_id, watch_id, replacement.epoch),
                ).fetchall()
                for grant in stale:
                    db.execute(
                        "DELETE FROM event_frontiers WHERE owner=? AND subscription=?",
                        (owner_id, grant["subscription"]),
                    )
                    db.execute(
                        "DELETE FROM event_replays WHERE owner=? AND subscription=?",
                        (owner_id, grant["subscription"]),
                    )
                db.execute(
                    "DELETE FROM event_subscriptions WHERE owner=? AND watch=? AND epoch!=?",
                    (owner_id, watch_id, replacement.epoch),
                )
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
        db.execute(
            "INSERT OR IGNORE INTO event_history(owner,event_id) VALUES(?,?)", (owner, event_id)
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
            row = self._delivery_row(db, owner_id, delivery_id)
            receipts = db.execute(
                "SELECT h.seq,d.state FROM event_history h JOIN deliveries d ON d.owner=h.owner AND d.event_id=h.event_id WHERE d.owner=? AND d.subscription=? ORDER BY h.seq",
                (owner_id, row["subscription"]),
            ).fetchall()
            position = 0
            for receipt in receipts:
                if receipt["state"] != "acked":
                    break
                position = receipt["seq"]
            db.execute(
                "INSERT INTO event_frontiers(owner,subscription,position) VALUES(?,?,?) ON CONFLICT(owner,subscription) DO UPDATE SET position=MAX(position,excluded.position)",
                (owner_id, row["subscription"], position),
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

    def _obligation_versions(self, db, owner_id, watch_id, ids):
        versions = {}
        ids = tuple(ids)
        for offset in range(0, len(ids), 500):
            chunk = ids[offset : offset + 500]
            rows = db.execute(
                "SELECT obligation,version FROM obligations WHERE owner=? AND watch=? AND obligation IN ("
                + ",".join("?" for _ in chunk)
                + ")",
                (owner_id, watch_id, *chunk),
            )
            versions.update((row[0], row[1]) for row in rows)
        return versions

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
            versions = self._obligation_versions(db, owner_id, watch_id, ids)
            return AnalysisSnapshot(
                owner_id,
                watch_id,
                epoch,
                watermark,
                tuple((value, versions.get(value, 0)) for value in ids),
            )

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
            versions = self._obligation_versions(
                db, snapshot.owner_id, snapshot.watch_id, expected
            )
            if any(
                versions.get(obligation, 0) != version for obligation, version in expected.items()
            ):
                return False
            evidence = tuple({seq for change in changes for seq in change.evidence})
            found = set()
            for offset in range(0, len(evidence), 500):
                chunk = evidence[offset : offset + 500]
                found.update(
                    row[0]
                    for row in db.execute(
                        "SELECT seq FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq IN ("
                        + ",".join("?" for _ in chunk)
                        + ")",
                        (snapshot.owner_id, snapshot.watch_id, snapshot.epoch, *chunk),
                    )
                )
            if len(found) != len(evidence):
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
                    data.get("confidence", 1.0),
                    data.get("due_at"),
                    data.get("due_origin"),
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
                "DELETE FROM event_frontiers WHERE owner=? AND subscription NOT IN (SELECT subscription FROM event_subscriptions WHERE owner=?)",
                (owner_id, owner_id),
            )
            expired = db.execute(
                "SELECT d.subscription,MAX(h.seq) AS floor FROM delivery_batches b JOIN deliveries d ON d.owner=b.owner AND d.event_id=b.event_id JOIN event_history h ON h.owner=b.owner AND h.event_id=b.event_id WHERE b.owner=? AND b.created<? AND EXISTS(SELECT 1 FROM event_subscriptions s WHERE s.owner=d.owner AND s.subscription=d.subscription) GROUP BY d.subscription",
                (owner_id, boundary),
            ).fetchall()
            for item in expired:
                db.execute(
                    "INSERT INTO event_frontiers(owner,subscription,floor) VALUES(?,?,?) ON CONFLICT(owner,subscription) DO UPDATE SET floor=MAX(floor,excluded.floor)",
                    (owner_id, item["subscription"], item["floor"]),
                )
            db.execute(
                "DELETE FROM delivery_batches WHERE owner=? AND created<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM source_events WHERE owner=? AND occurred<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM budget_buckets WHERE owner=? AND bucket<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM recipient_sources WHERE owner=? AND created<?", (owner_id, boundary)
            )
            db.execute(
                "DELETE FROM policy_global_budgets WHERE owner=? AND bucket<?",
                (owner_id, boundary),
            )
            return sum(row["count"] for row in rows)

    def policy_pending(self, owner, watch_id):
        with self._transaction() as db:
            watch = self._get(db, owner, watch_id)
            if not watch or watch.state != WatchState.ACTIVE:
                return None, (), None
            row = db.execute(
                "SELECT * FROM policy_progress WHERE owner=? AND watch=? AND epoch=?",
                (owner, watch_id, watch.epoch),
            ).fetchone()
            cursor = row["cursor"] if row else 0
            sources = tuple(
                self._source(r)
                for r in db.execute(
                    "SELECT * FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq>? ORDER BY seq",
                    (owner, watch_id, watch.epoch, cursor),
                )
            )
            return (
                watch,
                sources,
                datetime.fromisoformat(row["last_wake"]) if row and row["last_wake"] else None,
            )

    def dispatch_policy(
        self, owner, watch_id, epoch, sequences, *, deliveries, now, watch_limit, account_limit
    ):
        """Atomically fence, budget, deduplicate, enqueue and advance the settled cursor."""
        with self._transaction() as db:
            watch = self._active(db, owner, watch_id, epoch)
            current = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?", (owner, watch_id)
            ).fetchone()[0]
            if not sequences or current != max(sequences):
                return None
            rows = db.execute(
                "SELECT * FROM source_events WHERE owner=? AND watch=? AND epoch=? AND seq>? ORDER BY seq",
                (owner, watch_id, epoch, min(sequences) - 1),
            ).fetchall()
            if tuple(r["seq"] for r in rows) != tuple(sequences):
                return None
            cursor = db.execute(
                "SELECT cursor,last_wake FROM policy_progress WHERE owner=? AND watch=? AND epoch=?",
                (owner, watch_id, epoch),
            ).fetchone()
            if cursor and cursor[0] >= current:
                return None
            incoming = any(
                not self._source(r).record.metadata.get("outgoing", False) for r in rows
            )
            requests = (
                tuple(dict.fromkeys(deliveries))
                if incoming and watch.active.policy.wake_dot
                else ()
            )
            source_ids = {r["source_id"] for r in rows}
            unseen_by_recipient = {}
            for recipient in {r.recipient_id for r in requests}:
                known = {
                    r[0]
                    for r in db.execute(
                        "SELECT r.source_id FROM recipient_sources r JOIN source_events s ON r.owner=s.owner AND r.source_id=s.source_id WHERE s.owner=? AND s.watch=? AND s.epoch=? AND r.recipient=?",
                        (owner, watch_id, epoch, recipient),
                    )
                }
                unseen_by_recipient[recipient] = [r for r in rows if r["source_id"] not in known]
            requests = tuple(r for r in requests if unseen_by_recipient[r.recipient_id])
            if incoming and watch.active.policy.wake_dot and not deliveries:
                return None
            bucket = _time(now.replace(minute=0, second=0, microsecond=0))
            if requests:
                used_row = db.execute(
                    "SELECT used FROM policy_global_budgets WHERE owner=? AND bucket=?",
                    (owner, bucket),
                ).fetchone()
                used = used_row[0] if used_row else 0
                own = db.execute(
                    "SELECT used FROM budget_buckets WHERE owner=? AND watch=? AND name='packet' AND bucket=?",
                    (owner, watch_id, bucket),
                ).fetchone()
                if (
                    used + len(requests) > account_limit
                    or (own[0] if own else 0) + len(requests) > watch_limit
                ):
                    return None
                if (
                    db.execute(
                        "SELECT COUNT(*) FROM recipient_sources WHERE owner=?", (owner,)
                    ).fetchone()[0]
                    + len(source_ids) * len(requests)
                    > self.limits.max_deliveries
                ):
                    self._gap(db, owner, watch_id, "storage_limit")
                    return None
            event_id = None
            if requests:
                groups = {}
                for request in requests:
                    unseen = unseen_by_recipient[request.recipient_id]
                    selected = unseen[-100:]
                    payload = {
                        "source_ids": [r["source_id"] for r in selected],
                        "sequences": [r["seq"] for r in selected],
                        "has_more": len(unseen) > 100,
                    }
                    batch_id = sha256(
                        _json(
                            (
                                owner,
                                watch_id,
                                epoch,
                                tuple(payload["sequences"]),
                                payload["has_more"],
                            )
                        ).encode()
                    ).hexdigest()
                    groups.setdefault(batch_id, (payload, []))[1].append(request)
                if not self._capacity(
                    db,
                    owner,
                    watch_id,
                    batches=len(groups),
                    deliveries=len(requests),
                    size=sum(len(_json(payload).encode()) for payload, _ in groups.values()),
                ):
                    return None
                budget_exists = own is not None
                new_global = used_row is None
                keys = (
                    db.execute(
                        "SELECT COUNT(*) FROM budget_buckets WHERE owner=?", (owner,)
                    ).fetchone()[0]
                    + db.execute(
                        "SELECT COUNT(*) FROM policy_global_budgets WHERE owner=?", (owner,)
                    ).fetchone()[0]
                )
                if keys + int(not budget_exists) + int(new_global) > self.limits.max_budget_keys:
                    self._gap(db, owner, watch_id, "storage_limit")
                    return None
                for batch_id, (payload, group_requests) in groups.items():
                    self._enqueue(
                        db, owner, watch_id, epoch, batch_id, payload, group_requests, now
                    )
                    event_id = event_id or batch_id
                db.execute(
                    "INSERT INTO budget_buckets(owner,watch,name,bucket,used) VALUES(?,?,'packet',?,?) ON CONFLICT(owner,watch,name,bucket) DO UPDATE SET used=used+excluded.used",
                    (owner, watch_id, bucket, len(requests)),
                )
                db.execute(
                    "INSERT INTO policy_global_budgets(owner,bucket,used) VALUES(?,?,?) ON CONFLICT(owner,bucket) DO UPDATE SET used=used+excluded.used",
                    (owner, bucket, len(requests)),
                )
                for request in requests:
                    db.executemany(
                        "INSERT OR IGNORE INTO recipient_sources(owner,watch,recipient,source_id,created) VALUES(?,?,?,?,?)",
                        (
                            (owner, watch_id, request.recipient_id, sid, _time(now))
                            for sid in source_ids
                        ),
                    )
            last = _time(now) if requests else (cursor["last_wake"] if cursor else None)
            db.execute(
                "INSERT INTO policy_progress(owner,watch,epoch,cursor,last_wake) VALUES(?,?,?,?,?) ON CONFLICT(owner,watch) DO UPDATE SET epoch=excluded.epoch,cursor=excluded.cursor,last_wake=excluded.last_wake",
                (owner, watch_id, epoch, current, last),
            )
            return event_id

    def schedule_reminder(self, owner, watch_id, obligation_id, expected_version, due_at):
        """Trusted control-plane operation: an explicit one-shot recheck, never implied by a watch."""
        with self._transaction() as db:
            watch = self._get(db, owner, watch_id)
            if not watch:
                raise ScopeDenied("Unknown watch")
            self._active(db, owner, watch_id, watch.epoch)
            if not watch.active.policy.store_state or not watch.active.policy.wake_dot:
                raise ScopeDenied("Reminder rechecks require state and Dot permissions")
            row = db.execute(
                "SELECT version,document FROM obligations WHERE owner=? AND watch=? AND obligation=?",
                (owner, watch_id, obligation_id),
            ).fetchone()
            if not row or row["version"] != expected_version:
                raise ScopeDenied("Reminder requires the current obligation version")
            if json.loads(row["document"])["state"] not in (
                "awaiting_me",
                "awaiting_them",
                "snoozed",
            ):
                raise ValueError("Only open obligations can be rechecked")
            watermark = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?", (owner, watch_id)
            ).fetchone()[0]
            db.execute(
                "INSERT INTO reminder_rules(owner,watch,obligation,epoch,version,watermark,due) VALUES(?,?,?,?,?,?,?) ON CONFLICT(owner,watch,obligation) DO UPDATE SET epoch=excluded.epoch,version=excluded.version,watermark=excluded.watermark,due=excluded.due,fired=0",
                (
                    owner,
                    watch_id,
                    obligation_id,
                    watch.epoch,
                    expected_version,
                    watermark,
                    _time(due_at),
                ),
            )

    def due_reminders(self, owner, watch_id, *, now):
        with self._transaction() as db:
            watch = self._get(db, owner, watch_id)
            if (
                not watch
                or watch.state != WatchState.ACTIVE
                or not watch.active.policy.store_state
                or not watch.active.policy.wake_dot
            ):
                return ()
            watermark = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?", (owner, watch_id)
            ).fetchone()[0]
            rows = db.execute(
                "SELECT r.* FROM reminder_rules r JOIN obligations o ON r.owner=o.owner AND r.watch=o.watch AND r.obligation=o.obligation AND r.version=o.version WHERE r.owner=? AND r.watch=? AND r.epoch=? AND r.watermark=? AND r.due<=? AND r.fired=0 ORDER BY r.due LIMIT 100",
                (owner, watch_id, watch.epoch, watermark, _time(now)),
            ).fetchall()
            return tuple(
                (
                    r["obligation"],
                    r["due"],
                    AnalysisSnapshot(
                        owner, watch_id, watch.epoch, watermark, ((r["obligation"], r["version"]),)
                    ),
                )
                for r in rows
            )

    def complete_reminder(self, snapshot, obligation_id, due_at):
        """Consume only the exact recheck after its analysis was committed by the caller."""
        with self._transaction() as db:
            try:
                self._active(db, snapshot.owner_id, snapshot.watch_id, snapshot.epoch)
            except ScopeDenied:
                return False
            current = db.execute(
                "SELECT watermark FROM watches WHERE owner=? AND watch=?",
                (snapshot.owner_id, snapshot.watch_id),
            ).fetchone()[0]
            if current != snapshot.watermark:
                return False
            row = db.execute(
                "SELECT version,watermark,due FROM reminder_rules WHERE owner=? AND watch=? AND obligation=? AND epoch=?",
                (snapshot.owner_id, snapshot.watch_id, obligation_id, snapshot.epoch),
            ).fetchone()
            if (
                not row
                or row["due"] != due_at
                or row["watermark"] != snapshot.watermark
                or dict(snapshot.obligation_versions).get(obligation_id) != row["version"]
            ):
                return False
            db.execute(
                "UPDATE reminder_rules SET fired=1 WHERE owner=? AND watch=? AND obligation=?",
                (snapshot.owner_id, snapshot.watch_id, obligation_id),
            )
            return True

    def subscription_rows(self, owner, watch_id=None):
        with self._transaction() as db:
            return tuple(
                dict(r)
                for r in db.execute(
                    "SELECT * FROM event_subscriptions WHERE owner=?"
                    + (" AND watch=?" if watch_id else ""),
                    (owner, watch_id) if watch_id else (owner,),
                )
            )

    def save_subscription(self, owner, identity, subscription, watch_id, epoch, expires, document):
        with self._transaction() as db:
            self._active(db, owner, watch_id, epoch)
            db.execute(
                "DELETE FROM event_frontiers WHERE owner=? AND subscription NOT IN (SELECT subscription FROM event_subscriptions WHERE owner=?)",
                (owner, owner),
            )
            existing = db.execute(
                "SELECT 1 FROM event_subscriptions WHERE owner=? AND identity=?", (owner, identity)
            ).fetchone()
            if (
                not existing
                and db.execute(
                    "SELECT COUNT(*) FROM event_subscriptions WHERE owner=?", (owner,)
                ).fetchone()[0]
                >= 20
            ):
                raise ValueError("Subscription ceiling reached")
            db.execute(
                "INSERT INTO event_subscriptions VALUES(?,?,?,?,?,?,?) ON CONFLICT(owner,identity) DO UPDATE SET subscription=excluded.subscription,epoch=excluded.epoch,expires=excluded.expires,document=excluded.document",
                (owner, identity, subscription, watch_id, epoch, _time(expires), document),
            )

    def remove_subscription(self, owner, identity, *, now):
        from dataclasses import replace

        with self._transaction() as db:
            row = db.execute(
                "SELECT * FROM event_subscriptions WHERE owner=? AND identity=?", (owner, identity)
            ).fetchone()
            if not row:
                return
            db.execute(
                "UPDATE deliveries SET state='cancelled',token=NULL,lease_until=NULL WHERE owner=? AND subscription=? AND state IN ('pending','sending')",
                (owner, row["subscription"]),
            )
            db.execute(
                "DELETE FROM event_subscriptions WHERE owner=? AND identity=?", (owner, identity)
            )
            db.execute(
                "DELETE FROM event_frontiers WHERE owner=? AND subscription=?",
                (owner, row["subscription"]),
            )
            db.execute(
                "DELETE FROM event_replays WHERE owner=? AND subscription=?",
                (owner, row["subscription"]),
            )
            watch = self._get(db, owner, row["watch"])
            remaining = db.execute(
                "SELECT 1 FROM event_subscriptions WHERE owner=? AND watch=? AND epoch=? AND expires>?",
                (owner, row["watch"], row["epoch"], _time(now)),
            ).fetchone()
            if (
                watch
                and watch.state == WatchState.ACTIVE
                and watch.epoch == row["epoch"]
                and not remaining
                and not watch.active.policy.store_state
            ):
                paused = replace(
                    watch,
                    version=watch.version + 1,
                    epoch=watch.epoch + 1,
                    state=WatchState.PAUSED,
                )
                db.execute(
                    "UPDATE watches SET version=?,epoch=?,document=? WHERE owner=? AND watch=?",
                    (paused.version, paused.epoch, _watch_document(paused), owner, watch.watch_id),
                )
                db.execute(
                    "UPDATE deliveries SET state='cancelled',token=NULL,lease_until=NULL WHERE owner=? AND state IN ('pending','sending') AND event_id IN (SELECT event_id FROM delivery_batches WHERE owner=? AND watch=?)",
                    (owner, owner, watch.watch_id),
                )

    def event_position(self, owner, subscription):
        """Receipt frontier never moves beyond an unacknowledged retained delivery."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT h.seq,d.state FROM event_history h JOIN deliveries d ON d.owner=h.owner AND d.event_id=h.event_id WHERE d.owner=? AND d.subscription=? ORDER BY h.seq",
                (owner, subscription),
            ).fetchall()
            saved = db.execute(
                "SELECT position FROM event_frontiers WHERE owner=? AND subscription=?",
                (owner, subscription),
            ).fetchone()
            position = saved[0] if saved else 0
            for row in rows:
                if row["state"] != "acked":
                    return min(position, row["seq"] - 1)
                position = max(position, row["seq"])
            return position

    def event_metadata(self, owner, event_id):
        with self._transaction() as db:
            row = db.execute(
                "SELECT b.created,h.seq FROM delivery_batches b JOIN event_history h ON h.owner=b.owner AND h.event_id=b.event_id WHERE b.owner=? AND b.event_id=?",
                (owner, event_id),
            ).fetchone()
            return dict(row) if row else None

    def replay_events(self, owner, subscription, after):
        with self._transaction() as db:
            rows = db.execute(
                "SELECT h.seq,d.event_id,d.state FROM event_history h JOIN deliveries d ON d.owner=h.owner AND d.event_id=h.event_id WHERE d.owner=? AND d.subscription=? ORDER BY h.seq LIMIT 1001",
                (owner, subscription),
            ).fetchall()
            selected = tuple(dict(r) for r in rows if r["seq"] > after)
            return selected[:100], len(selected) > 100

    def request_replay(self, owner, subscription, after, *, now):
        """Stage bounded retained receipts; actual repeat POSTs share packet budgets."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT h.seq,d.event_id FROM event_history h JOIN deliveries d ON d.owner=h.owner AND d.event_id=h.event_id WHERE d.owner=? AND d.subscription=? ORDER BY h.seq",
                (owner, subscription),
            ).fetchall()
            saved = db.execute(
                "SELECT floor FROM event_frontiers WHERE owner=? AND subscription=?",
                (owner, subscription),
            ).fetchone()
            floor = saved[0] if saved else 0
            selected = [r for r in rows if r["seq"] > after]
            for row in selected[:100]:
                db.execute(
                    "INSERT OR IGNORE INTO event_replays VALUES(?,?,?)",
                    (owner, subscription, row["event_id"]),
                )
            return bool((floor and after <= floor) or len(selected) > 100)

    def _packet_gate(self, db, owner, watch, requests, now):
        from .policies import _quiet

        policy = watch.active.policy
        if not policy.wake_dot or _quiet(policy, now):
            return False
        progress = db.execute(
            "SELECT last_wake FROM policy_progress WHERE owner=? AND watch=? AND epoch=?",
            (owner, watch.watch_id, watch.epoch),
        ).fetchone()
        if (
            progress
            and progress[0]
            and (now - datetime.fromisoformat(progress[0])).total_seconds()
            < policy.min_wake_seconds
        ):
            return False
        bucket = _time(now.replace(minute=0, second=0, microsecond=0))
        own = db.execute(
            "SELECT used FROM budget_buckets WHERE owner=? AND watch=? AND name='packet' AND bucket=?",
            (owner, watch.watch_id, bucket),
        ).fetchone()
        total = db.execute(
            "SELECT used FROM policy_global_budgets WHERE owner=? AND bucket=?", (owner, bucket)
        ).fetchone()
        count = len(set(requests))
        if (own[0] if own else 0) + count > min(
            policy.wakes_per_hour, policy.notifications_per_hour
        ) or (total[0] if total else 0) + count > min(
            policy.account_wakes_per_hour, policy.account_notifications_per_hour
        ):
            return False
        keys = (
            db.execute("SELECT COUNT(*) FROM budget_buckets WHERE owner=?", (owner,)).fetchone()[0]
            + db.execute(
                "SELECT COUNT(*) FROM policy_global_budgets WHERE owner=?", (owner,)
            ).fetchone()[0]
        )
        if keys + int(own is None) + int(total is None) > self.limits.max_budget_keys:
            return False
        db.execute(
            "INSERT INTO budget_buckets(owner,watch,name,bucket,used) VALUES(?,?,'packet',?,?) ON CONFLICT(owner,watch,name,bucket) DO UPDATE SET used=used+excluded.used",
            (owner, watch.watch_id, bucket, count),
        )
        db.execute(
            "INSERT INTO policy_global_budgets VALUES(?,?,?) ON CONFLICT(owner,bucket) DO UPDATE SET used=used+excluded.used",
            (owner, bucket, count),
        )
        db.execute(
            "INSERT INTO policy_progress(owner,watch,epoch,cursor,last_wake) VALUES(?,?,?,0,?) ON CONFLICT(owner,watch) DO UPDATE SET epoch=excluded.epoch,last_wake=excluded.last_wake",
            (owner, watch.watch_id, watch.epoch, _time(now)),
        )
        return True

    def dispatch_control(self, owner, watch_id, epoch, event_id, payload, deliveries, *, now):
        with self._transaction() as db:
            watch = self._active(db, owner, watch_id, epoch)
            requests = tuple(set(deliveries))
            if (
                not requests
                or db.execute(
                    "SELECT 1 FROM delivery_batches WHERE owner=? AND event_id=?",
                    (owner, event_id),
                ).fetchone()
            ):
                return None
            if not self._capacity(
                db,
                owner,
                watch_id,
                batches=1,
                deliveries=len(requests),
                size=len(_json(payload).encode()),
            ):
                return None
            if not self._packet_gate(db, owner, watch, requests, now):
                return None
            self._enqueue(db, owner, watch_id, epoch, event_id, payload, requests, now)
            return event_id

    def dispatch_replays(self, owner, subscription, *, now):
        with self._transaction() as db:
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_replays'"
            ).fetchone()
            if not exists:
                return
            rows = db.execute(
                "SELECT d.*,b.watch,b.epoch FROM event_replays r JOIN deliveries d ON d.owner=r.owner AND d.subscription=r.subscription AND d.event_id=r.event_id JOIN delivery_batches b ON b.owner=d.owner AND b.event_id=d.event_id WHERE r.owner=? AND r.subscription=? ORDER BY d.due LIMIT 100",
                (owner, subscription),
            ).fetchall()
            for row in rows:
                try:
                    watch = self._active(db, owner, row["watch"], row["epoch"])
                except ScopeDenied:
                    db.execute(
                        "DELETE FROM event_replays WHERE owner=? AND subscription=? AND event_id=?",
                        (owner, subscription, row["event_id"]),
                    )
                    continue
                if row["state"] == "acked":
                    if not self._packet_gate(
                        db, owner, watch, (DeliveryRequest(row["recipient"], subscription),), now
                    ):
                        return
                    db.execute(
                        "UPDATE deliveries SET state='pending',attempts=0,due=?,token=NULL,lease_until=NULL WHERE delivery_id=?",
                        (_time(now), row["delivery_id"]),
                    )
                db.execute(
                    "DELETE FROM event_replays WHERE owner=? AND subscription=? AND event_id=?",
                    (owner, subscription, row["event_id"]),
                )

    def record_attempt_receipt(self, owner, delivery_id, *, now, category):
        """Bounded operational receipt even if revocation invalidates the claim token."""
        if category not in (
            "accepted",
            "http_4xx",
            "http_5xx",
            "timeout",
            "tls_error",
            "connection_refused",
            "challenge_failed",
        ):
            raise ValueError("Safe receipt category required")
        with self._transaction() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS event_attempt_receipts(owner TEXT NOT NULL, delivery TEXT NOT NULL, occurred TEXT NOT NULL, category TEXT NOT NULL, PRIMARY KEY(owner,delivery))"
            )
            db.execute(
                "INSERT INTO event_attempt_receipts VALUES(?,?,?,?) ON CONFLICT(owner,delivery) DO UPDATE SET occurred=excluded.occurred,category=excluded.category",
                (owner, delivery_id, _time(now), category),
            )
            if category != "accepted":
                delivery = self._delivery_row(db, owner, delivery_id)
                watch = self._get(db, owner, delivery["watch"]) if delivery else None
                if watch and watch.state == WatchState.ACTIVE and watch.epoch == delivery["epoch"]:
                    reason = "callback_" + category
                    previous = db.execute(
                        "SELECT reason FROM health WHERE owner=? AND watch=?",
                        (owner, watch.watch_id),
                    ).fetchone()
                    if previous and previous[0] != reason:
                        self._gap(db, owner, watch.watch_id, reason)
            db.execute(
                "DELETE FROM event_attempt_receipts WHERE owner=? AND delivery NOT IN (SELECT delivery_id FROM deliveries WHERE owner=?)",
                (owner, owner),
            )

    def reserve_delivery_retry(self, owner, delivery_id, token, *, now):
        """Retry attempts consume actual POST capacity, sharing quiet/minimum-wake gates."""
        from datetime import timedelta

        with self._transaction() as db:
            if not self._current_delivery(db, owner, delivery_id, token):
                return False
            row = self._delivery_row(db, owner, delivery_id)
            if row["attempts"] <= 1:
                return True  # First attempt was reserved atomically at publication.
            watch = self._active(db, owner, row["watch"], row["epoch"])
            if self._packet_gate(
                db, owner, watch, (DeliveryRequest(row["recipient"], row["subscription"]),), now
            ):
                return True
            db.execute(
                "UPDATE deliveries SET state='pending',attempts=attempts-1,due=?,token=NULL,lease_until=NULL WHERE delivery_id=?",
                (_time(now + timedelta(seconds=60)), delivery_id),
            )
            return False
