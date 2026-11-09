"""Immutable watch contracts; Telegram resolution and delivery live in adapters."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Tuple


@dataclass(frozen=True)
class ScopeTarget:
    """A resolved account and Telethon marked peer ID, never an alias or raw channel ID.

    Construct these at the trusted resolver boundary. Numeric shape cannot prove
    resolution; callers must not convert unresolved user input directly to this type.
    A topic ID selects exactly that forum topic; None explicitly selects the chat.
    """

    account_id: str
    marked_chat_id: int
    topic_id: Optional[int] = None

    def __post_init__(self):
        if (
            not isinstance(self.account_id, str)
            or not self.account_id.strip()
            or "*" in self.account_id
        ):
            raise ValueError("An explicit resolved account ID is required")
        if type(self.marked_chat_id) is not int or self.marked_chat_id == 0:
            raise ValueError("A nonzero canonical marked peer ID is required")
        if self.topic_id is not None:
            if type(self.topic_id) is not int or self.topic_id <= 0:
                raise ValueError("A resolved positive topic ID is required")
            if self.marked_chat_id >= -1000000000000:
                raise ValueError("Topics require a marked channel peer ID")


@dataclass(frozen=True)
class FolderSnapshot:
    """Selection provenance only; changing a Telegram folder cannot alter this tuple."""

    folder_id: int
    targets: Tuple[ScopeTarget, ...]

    def __post_init__(self):
        if type(self.folder_id) is not int or self.folder_id < 0:
            raise ValueError("A concrete folder ID is required")
        object.__setattr__(self, "targets", tuple(self.targets))
        if any(not isinstance(target, ScopeTarget) for target in self.targets):
            raise ValueError("Folder snapshots contain resolved targets only")


@dataclass(frozen=True)
class WatchScope:
    targets: Tuple[ScopeTarget, ...]
    folder_id: Optional[int] = None

    def __post_init__(self):
        object.__setattr__(self, "targets", tuple(self.targets))
        if any(not isinstance(target, ScopeTarget) for target in self.targets):
            raise ValueError("Scope contains resolved targets only")
        if len(set(self.targets)) != len(self.targets):
            raise ValueError("Duplicate scope targets are not allowed")
        if self.folder_id is not None and (type(self.folder_id) is not int or self.folder_id < 0):
            raise ValueError("Invalid folder snapshot ID")

    @classmethod
    def from_folder_snapshot(cls, snapshot: FolderSnapshot) -> "WatchScope":
        return cls(snapshot.targets, snapshot.folder_id)


@dataclass(frozen=True)
class Capability:
    name: str
    event_class: str
    available: bool
    coverage: str
    delivery_modes: Tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(self, "delivery_modes", tuple(self.delivery_modes))


@dataclass(frozen=True)
class WatchPolicy:
    """Proposed quiet defaults. Observation grants no outbound or media permission."""

    # Evidence/obligation tracking; stateless batches do not require it.
    store_state: bool = False
    wake_dot: bool = False
    notify: bool = False
    settle_seconds: int = 45
    max_settle_seconds: int = 180
    min_wake_seconds: int = 300
    wakes_per_hour: int = 6
    account_wakes_per_hour: int = 20
    notifications_per_hour: int = 3
    account_notifications_per_hour: int = 10
    quiet_start_minute: Optional[int] = None
    quiet_end_minute: Optional[int] = None
    quiet_timezone: str = "Europe/Samara"
    own_outgoing_silent: bool = True
    reminders: bool = False
    transcribe_media: bool = False
    send_messages: bool = False

    def __post_init__(self):
        for name in (
            "store_state",
            "wake_dot",
            "notify",
            "own_outgoing_silent",
            "reminders",
            "transcribe_media",
            "send_messages",
        ):
            if type(getattr(self, name)) is not bool:
                raise ValueError("Policy switches must be booleans")
        for name in ("settle_seconds", "max_settle_seconds", "min_wake_seconds"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError("Timing intervals must be positive integer seconds")
        for name in (
            "wakes_per_hour",
            "account_wakes_per_hour",
            "notifications_per_hour",
            "account_notifications_per_hour",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError("Positive hourly budgets required")
        if (self.quiet_start_minute is None) != (self.quiet_end_minute is None):
            raise ValueError("Both quiet-hour bounds are required")
        if self.quiet_start_minute is not None and any(
            type(v) is not int or not 0 <= v < 1440
            for v in (self.quiet_start_minute, self.quiet_end_minute)
        ):
            raise ValueError("Quiet-hour bounds are local minutes")
        from zoneinfo import ZoneInfo

        ZoneInfo(self.quiet_timezone)
        if self.max_settle_seconds < self.settle_seconds:
            raise ValueError("Maximum settling must cover the quiet interval")
        if self.notify and not self.wake_dot:
            raise ValueError("Notifications require Dot analysis")
        if self.reminders or self.transcribe_media or self.send_messages:
            raise ValueError("Reminders, transcription and sending require separate future rules")
        if not self.own_outgoing_silent:
            raise ValueError("Outgoing-only delivery is not available")


class WatchState(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    REVOKED = "revoked"


@dataclass(frozen=True)
class WatchRevision:
    """One revision pins label, scope, policy and capabilities together."""

    revision: int
    label: str
    scope: WatchScope
    policy: WatchPolicy
    capabilities: Tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(self, "capabilities", tuple(self.capabilities))


@dataclass(frozen=True)
class Watch:
    watch_id: str
    owner_id: str
    version: int
    epoch: int
    state: WatchState
    active: Optional[WatchRevision] = None
    draft: Optional[WatchRevision] = None
    boundary: Optional[datetime] = None
    schema_version: int = 1


@dataclass(frozen=True)
class WatchPreview:
    watch_id: str
    revision: int
    label: str
    scope: WatchScope
    policy: WatchPolicy
    capabilities: Tuple[Capability, ...]
    added_targets: Tuple[ScopeTarget, ...]
    removed_targets: Tuple[ScopeTarget, ...]
    tracked_state_changes: Tuple[str, ...]
    dot_batches: Tuple[str, ...]
    potential_notifications: Tuple[str, ...]
    # Minimal source/delivery metadata is separate from obligation tracking.
    source_metadata_retained: bool = True
    start_boundary: str = "activation time; new events only"
