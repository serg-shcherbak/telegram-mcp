"""Owner-bound watch operations, without framework registration or Telegram I/O.

The caller is the trusted control plane. In a future tool adapter, activation must
be a ChatGPT write-gated operation after showing the exact preview. Revision
checks prevent stale activation; neither a revision nor a boolean proves consent.
"""

from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable, Optional, Protocol, Tuple
from uuid import uuid4

from .models import (
    Capability,
    ScopeTarget,
    Watch,
    WatchPolicy,
    WatchPreview,
    WatchRevision,
    WatchScope,
    WatchState,
)
from .scope import CAPABILITY_CATALOG, ScopeDenied, matches_scope


class RevisionConflict(ValueError):
    pass


class UnsupportedCapability(ValueError):
    pass


class WatchNotFound(LookupError):
    pass


class WatchRepository(Protocol):
    """Owner-scoped storage; CAS is the sole atomic mutation boundary.

    get/list return immutable snapshots. compare_and_swap compares Watch.version
    (None means absent), then replaces or deletes atomically and returns success.
    A durable implementation must also invalidate pending work for older epochs
    inside that same transaction when epoch changes or the watch is deleted.
    A missing watch is always denied. No repository operation may cross owners.
    """

    def get(self, owner_id: str, watch_id: str) -> Optional[Watch]: ...

    def list(self, owner_id: str) -> Tuple[Watch, ...]: ...

    def compare_and_swap(
        self,
        owner_id: str,
        watch_id: str,
        expected_version: Optional[int],
        replacement: Optional[Watch],
    ) -> bool: ...


class WatchManager:
    """Bind trusted deployment identity once; methods never accept an owner.

    A draft revision pins the entire scope and policy together. Activating it
    invalidates the previous epoch. Pause, resume and revoke do likewise; resume
    starts a fresh new-events boundary rather than replaying the paused interval.
    Labels are display data only. This API never reads history or delivers work.
    """

    def __init__(
        self,
        repository: WatchRepository,
        *,
        owner_id: str,
        account_ids: Tuple[str, ...],
        catalog: Tuple[Capability, ...] = CAPABILITY_CATALOG,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        id_factory: Callable[[], str] = lambda: str(uuid4()),
    ):
        if not owner_id or not isinstance(owner_id, str):
            raise ValueError("Trusted owner identity is required")
        self._repository = repository
        self._owner_id = owner_id
        self._account_ids = frozenset(account_ids)
        if not self._account_ids:
            raise ValueError("Trusted account identities are required")
        self._catalog = {item.name: item for item in catalog}
        if len(self._catalog) != len(catalog):
            raise ValueError("Duplicate capability names")
        self._clock = clock
        self._id_factory = id_factory

    def catalog(self) -> Tuple[Capability, ...]:
        return tuple(self._catalog.values())

    def list(self) -> Tuple[Watch, ...]:
        return self._repository.list(self._owner_id)

    def get(self, watch_id: str) -> Watch:
        watch = self._repository.get(self._owner_id, watch_id)
        if watch is None:
            raise WatchNotFound(watch_id)
        return watch

    def _save(self, previous: Optional[Watch], updated: Watch) -> Watch:
        expected = previous.version if previous is not None else None
        if not self._repository.compare_and_swap(
            self._owner_id, updated.watch_id, expected, updated
        ):
            raise RevisionConflict("Watch changed; get it and preview the current draft")
        return updated

    def _version(self, watch: Watch, expected_version: int) -> None:
        if type(expected_version) is not int or watch.version != expected_version:
            raise RevisionConflict("Watch changed; get the current version")

    def _validate(self, revision: WatchRevision) -> None:
        if not revision.scope.targets:
            raise ScopeDenied("Select at least one resolved chat or topic")
        if any(target.account_id not in self._account_ids for target in revision.scope.targets):
            raise ScopeDenied("Account is outside the trusted deployment")
        if not revision.capabilities:
            raise UnsupportedCapability("Select at least one available capability")
        if len(set(revision.capabilities)) != len(revision.capabilities):
            raise ValueError("Duplicate capabilities")
        for name in revision.capabilities:
            capability = self._catalog.get(name)
            if capability is None or not capability.available:
                raise UnsupportedCapability(f"Capability unavailable: {name}")

    def prepare(
        self,
        *,
        label: str,
        scope: WatchScope,
        capabilities: Tuple[str, ...],
        policy: WatchPolicy = WatchPolicy(),
        watch_id: Optional[str] = None,
        expected_version: Optional[int] = None,
    ) -> Watch:
        """Create or replace a draft; an existing active revision stays unchanged."""
        if not isinstance(label, str) or not label.strip():
            raise ValueError("A display label is required")
        if not isinstance(scope, WatchScope) or not isinstance(policy, WatchPolicy):
            raise ValueError("Resolved scope and validated policy objects are required")
        previous = self.get(watch_id) if watch_id is not None else None
        if previous is None:
            if expected_version is not None:
                raise RevisionConflict("A new watch has no previous version")
            watch_id = self._id_factory()
            revision_number = 1
        else:
            self._version(previous, expected_version)
            if previous.state == WatchState.REVOKED:
                raise ValueError("A revoked watch cannot be changed; create a new watch")
            revision_number = (
                max(
                    item.revision for item in (previous.active, previous.draft) if item is not None
                )
                + 1
            )
        draft = WatchRevision(revision_number, label, scope, policy, capabilities)
        self._validate(draft)
        updated = (
            replace(previous, version=previous.version + 1, draft=draft)
            if previous
            else Watch(watch_id, self._owner_id, 1, 0, WatchState.DRAFT, draft=draft)
        )
        return self._save(previous, updated)

    def preview(self, watch_id: str, *, expected_revision: int) -> WatchPreview:
        """Describe the exact draft and effects; no history fetch, storage or delivery."""
        watch = self.get(watch_id)
        draft = watch.draft
        if (
            draft is None
            or type(expected_revision) is not int
            or draft.revision != expected_revision
        ):
            raise RevisionConflict("Preview requires the exact current draft revision")
        self._validate(draft)
        previous = watch.active.scope.targets if watch.active else ()
        selected = draft.scope.targets
        return WatchPreview(
            watch_id=watch_id,
            revision=draft.revision,
            label=draft.label,
            scope=draft.scope,
            policy=draft.policy,
            capabilities=tuple(self._catalog[name] for name in draft.capabilities),
            added_targets=tuple(target for target in selected if target not in previous),
            removed_targets=tuple(target for target in previous if target not in selected),
            tracked_state_changes=draft.capabilities if draft.policy.store_state else (),
            dot_batches=draft.capabilities if draft.policy.wake_dot else (),
            potential_notifications=draft.capabilities if draft.policy.notify else (),
        )

    def _now(self) -> datetime:
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("The trusted clock must return an aware datetime")
        return now

    def activate(self, watch_id: str, *, expected_revision: int, expected_version: int) -> Watch:
        """Activate the exact immutable scope+policy revision; not a consent assertion."""
        watch = self.get(watch_id)
        if type(expected_revision) is not int:
            raise RevisionConflict("An exact revision is required")
        if (
            watch.state == WatchState.ACTIVE
            and watch.draft is None
            and watch.active.revision == expected_revision
        ):
            return watch
        self._version(watch, expected_version)
        if watch.state == WatchState.REVOKED:
            raise ValueError("A revoked watch cannot be activated")
        if watch.draft is None or watch.draft.revision != expected_revision:
            raise RevisionConflict("Activation requires the exact current draft revision")
        self._validate(watch.draft)
        return self._save(
            watch,
            replace(
                watch,
                version=watch.version + 1,
                epoch=watch.epoch + 1,
                state=WatchState.ACTIVE,
                active=watch.draft,
                draft=None,
                boundary=self._now(),
            ),
        )

    def pause(self, watch_id: str, *, expected_version: int) -> Watch:
        watch = self.get(watch_id)
        self._version(watch, expected_version)
        if watch.state == WatchState.PAUSED:
            return watch
        if watch.state != WatchState.ACTIVE:
            raise ValueError("Only an active watch can be paused")
        return self._save(
            watch,
            replace(
                watch,
                version=watch.version + 1,
                epoch=watch.epoch + 1,
                state=WatchState.PAUSED,
            ),
        )

    def resume(self, watch_id: str, *, expected_version: int) -> Watch:
        watch = self.get(watch_id)
        self._version(watch, expected_version)
        if watch.state != WatchState.PAUSED:
            raise ValueError("Only a paused watch can be resumed")
        self._validate(watch.active)
        return self._save(
            watch,
            replace(
                watch,
                version=watch.version + 1,
                epoch=watch.epoch + 1,
                state=WatchState.ACTIVE,
                boundary=self._now(),
            ),
        )

    def revoke(self, watch_id: str, *, expected_version: int) -> Watch:
        watch = self.get(watch_id)
        self._version(watch, expected_version)
        if watch.state == WatchState.REVOKED:
            return watch
        return self._save(
            watch,
            replace(
                watch,
                version=watch.version + 1,
                epoch=watch.epoch + 1,
                state=WatchState.REVOKED,
                draft=None,
            ),
        )

    def delete(self, watch_id: str, *, expected_version: int) -> None:
        """Delete a revoked watch; the store deletes its dependent user data atomically."""
        watch = self.get(watch_id)
        self._version(watch, expected_version)
        if watch.state != WatchState.REVOKED:
            raise ValueError("Revoke the watch before deleting its data")
        if not self._repository.compare_and_swap(self._owner_id, watch_id, watch.version, None):
            raise RevisionConflict("Watch changed before deletion")

    def permits(self, watch_id: str, target: ScopeTarget, *, expected_epoch: int) -> bool:
        """Recheck at every processing boundary; later storage must do so atomically.

        This snapshot predicate alone is not a transaction fence for external I/O.
        A durable adapter must fence writes and invalidate queued work using epoch.
        """
        try:
            watch = self.get(watch_id)
        except WatchNotFound:
            return False
        return (
            watch.state == WatchState.ACTIVE
            and type(expected_epoch) is int
            and watch.epoch == expected_epoch
            and watch.active is not None
            and matches_scope(watch.active.scope, target)
        )
