"""Read-only context boundary for a separately authorized deployment profile.

The deployment must bind this profile to authenticated connection authority;
constructing it from tool arguments does not establish that authority. This
module registers no tools and does not grant access to the broad manual reader.
Callers must pin an analysis snapshot before awaiting read(), then conditionally
commit conclusions with the store: a successful read is not a freshness lock.
"""

from dataclasses import asdict, dataclass, field
import json
from typing import Mapping, Optional, Tuple

from telethon import utils
from telethon.errors import RPCError

from .ingress import message_topic
from .models import ScopeTarget, WatchState
from .scope import ScopeDenied, matches_scope
from .store import Obligation

MAX_SOURCE_IDS = 100
MAX_MESSAGE_BYTES = 32 * 1024
MAX_CONTEXT_TEXT_BYTES = 100 * 1024
MAX_CONTEXT_BYTES = 256 * 1024


def _bounded_payload(value):
    if len(json.dumps(asdict(value), ensure_ascii=False).encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise ValueError("Serialized context exceeds 256 KiB; request fewer sources")
    return value


@dataclass(frozen=True)
class ContextProfile:
    """Trusted connection authority, supplied outside caller-controlled arguments."""

    owner_id: str
    allowed_watch_ids: Tuple[str, ...]

    def __post_init__(self):
        if isinstance(self.allowed_watch_ids, str):
            raise ValueError("Explicit watch ID collection required")
        object.__setattr__(self, "allowed_watch_ids", tuple(self.allowed_watch_ids))
        if not self.allowed_watch_ids or any(
            not isinstance(value, str)
            or not value.strip()
            or "*" in value
            or len(value.encode("utf-8")) > 256
            for value in (self.owner_id, *self.allowed_watch_ids)
        ):
            raise ValueError("Explicit bounded owner and nonempty watch allowlist required")


@dataclass(frozen=True)
class ContextMessage:
    sequence: int
    target: ScopeTarget
    message_id: Optional[int]
    kind: str
    outgoing: bool = False
    text: str = ""
    media_kind: Optional[str] = None
    gap: Optional[str] = None
    truncated: bool = False
    trust: str = field(default="untrusted", init=False)


@dataclass(frozen=True)
class UntrustedContext:
    watch_id: str
    epoch: int
    messages: Tuple[ContextMessage, ...]
    has_more: bool = False
    trust: str = field(default="untrusted", init=False)


@dataclass(frozen=True)
class UntrustedObligations:
    watch_id: str
    epoch: int
    obligations: Tuple[Obligation, ...]
    has_more: bool = False
    trust: str = field(default="untrusted", init=False)


class ScopedContextReader:
    """Fetch only journal-selected IDs through existing account clients.

    clientresolver is a trusted account-to-existing-client mapping, never a
    factory that creates a second Telegram session. No Telegram object, session,
    entity/access hash or arbitrary source metadata crosses the return boundary.
    """

    def __init__(self, store, profile: ContextProfile, clientresolver: Mapping):
        if not isinstance(profile, ContextProfile):
            raise ValueError("A trusted context profile is required")
        self.store = store
        self._profile = profile
        self._clients = dict(clientresolver)

    @property
    def profile(self):
        return self._profile

    def _watch(self, watch_id, epoch=None):
        if watch_id not in self.profile.allowed_watch_ids:
            raise ScopeDenied("Watch is not authorized by context profile")
        watch = self.store.get(self.profile.owner_id, watch_id)
        if (
            watch is None
            or watch.owner_id != self.profile.owner_id
            or watch.state != WatchState.ACTIVE
            or watch.active is None
            or not watch.active.scope.targets
            or (epoch is not None and watch.epoch != epoch)
        ):
            raise ScopeDenied("Current active owned watch required")
        return watch

    def _source(self, watch, sequence):
        rows = self.store.get_sources(
            self.profile.owner_id, watch.watch_id, after=sequence - 1, limit=1
        )
        if not rows:
            raise ScopeDenied("Current in-scope journal source required")
        source = rows[0]
        if (
            source.sequence != sequence
            or source.watch_id != watch.watch_id
            or source.epoch != watch.epoch
            or not matches_scope(watch.active.scope, source.record.target)
        ):
            raise ScopeDenied("Current in-scope journal source required")
        return source

    async def read(self, watch_id, source_sequences):
        watch = self._watch(watch_id)
        sequences = []
        for sequence in source_sequences:
            if type(sequence) is not int or sequence <= 0:
                raise ValueError("Positive journal sequences required")
            sequences.append(sequence)
            if len(sequences) > MAX_SOURCE_IDS:
                raise ValueError("At most 100 journal sequences per context")
        if not sequences or len(set(sequences)) != len(sequences):
            raise ValueError("Nonempty distinct journal sequences required")
        sources = tuple(self._source(watch, sequence) for sequence in sequences)
        groups = {}
        for source in sources:
            record = source.record
            if record.message_id is not None and record.target.account_id in self._clients:
                key = (record.target.account_id, record.target.marked_chat_id)
                groups.setdefault(key, []).append(source)
        fetched = {}
        for (account, peer), group in groups.items():
            current = self._watch(watch_id, watch.epoch)
            for source in group:
                self._source(current, source.sequence)
            ids = list(dict.fromkeys(source.record.message_id for source in group))
            try:
                messages = await self._clients[account].get_messages(peer, ids=ids)
            except (RPCError, OSError):
                messages = []
            current = self._watch(watch_id, watch.epoch)
            for source in group:
                self._source(current, source.sequence)
            by_id = {}
            for message in messages:
                mid = getattr(message, "id", None)
                if mid in ids:
                    # Ambiguous duplicate responses cannot establish message identity.
                    by_id[mid] = None if mid in by_id else message
            fetched[(account, peer)] = by_id
        output = []
        remaining = MAX_CONTEXT_TEXT_BYTES
        for source in sources:
            current = self._watch(watch_id, watch.epoch)
            self._source(current, source.sequence)
            record = source.record
            base = dict(
                sequence=source.sequence,
                target=record.target,
                message_id=record.message_id,
                kind=record.kind,
            )
            if record.message_id is None:
                output.append(ContextMessage(**base, gap="no_message_reference"))
                continue
            client = self._clients.get(record.target.account_id)
            if client is None:
                output.append(ContextMessage(**base, gap="account_unavailable"))
                continue
            message = fetched[(record.target.account_id, record.target.marked_chat_id)].get(
                record.message_id
            )
            if message is None or getattr(message, "peer_id", None) is None:
                output.append(ContextMessage(**base, gap="unavailable"))
                continue
            try:
                target = ScopeTarget(
                    record.target.account_id,
                    utils.get_peer_id(message.peer_id),
                    message_topic(message),
                )
            except (TypeError, ValueError):
                target = None
            if (
                message.id != record.message_id
                or target != record.target
                or not matches_scope(current.active.scope, target)
            ):
                output.append(ContextMessage(**base, gap="identity_mismatch"))
                continue
            text = getattr(message, "message", None) or ""
            encoded = text.encode("utf-8")
            bounded = encoded[: min(remaining, MAX_MESSAGE_BYTES)].decode("utf-8", "ignore")
            remaining -= len(bounded.encode("utf-8"))
            truncated = len(encoded) > len(bounded.encode("utf-8"))
            media = (
                "voice"
                if getattr(message, "voice", False)
                else "attachment" if getattr(message, "media", None) else None
            )
            output.append(
                ContextMessage(
                    **base,
                    text=bounded,
                    outgoing=bool(getattr(message, "out", False)),
                    media_kind=media,
                    truncated=truncated,
                )
            )
        self._watch(watch_id, watch.epoch)
        return _bounded_payload(
            UntrustedContext(
                watch_id, watch.epoch, tuple(output), any(item.truncated for item in output)
            )
        )

    def get_obligations(self, watch_id):
        """Return bounded conclusions only when all evidence remains in scope."""
        watch = self._watch(watch_id)
        output = []
        for obligation in self.store.get_obligations(self.profile.owner_id, watch_id):
            if not obligation.evidence:
                continue
            try:
                for sequence in obligation.evidence:
                    self._source(watch, sequence)
            except ScopeDenied:
                continue
            output.append(obligation)
            if len(output) > MAX_SOURCE_IDS:
                break
        self._watch(watch_id, watch.epoch)
        return _bounded_payload(
            UntrustedObligations(
                watch_id, watch.epoch, tuple(output[:MAX_SOURCE_IDS]), len(output) > MAX_SOURCE_IDS
            )
        )
