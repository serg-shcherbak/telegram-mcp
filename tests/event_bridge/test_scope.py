"""Default-deny scope and framework-neutral watch control contracts."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from telegram_mcp.event_bridge.models import (
    Capability,
    FolderSnapshot,
    ScopeTarget,
    WatchPolicy,
    WatchScope,
    WatchState,
)
from telegram_mcp.event_bridge.scope import CAPABILITY_CATALOG, ScopeDenied, matches_scope
from telegram_mcp.event_bridge.tools import (
    RevisionConflict,
    UnsupportedCapability,
    WatchManager,
    WatchNotFound,
)

NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)
PEER = -1001234567890
EVENT = "telegram.message.created"


class MemoryRepository:
    """The durable adapter must implement this owner-scoped atomic CAS contract."""

    def __init__(self):
        self.rows = {}

    def get(self, owner_id, watch_id):
        return self.rows.get((owner_id, watch_id))

    def list(self, owner_id):
        return tuple(row for (owner, _), row in self.rows.items() if owner == owner_id)

    def compare_and_swap(self, owner_id, watch_id, expected_version, replacement):
        key = (owner_id, watch_id)
        current = self.rows.get(key)
        version = current.version if current else None
        if version != expected_version:
            return False
        if replacement is None:
            self.rows.pop(key, None)
        else:
            self.rows[key] = replacement
        return True


@pytest.fixture
def controls():
    clock = [NOW]
    repository = MemoryRepository()
    catalog = (Capability(EVENT, "message", True, "observed updates only", ("batch",)),)
    manager = WatchManager(
        repository,
        owner_id="trusted-owner",
        account_ids=("primary",),
        catalog=catalog,
        clock=lambda: clock[0],
        id_factory=lambda: "watch-1",
    )
    return manager, repository, clock


def scope(topic_id=None):
    return WatchScope((ScopeTarget("primary", PEER, topic_id),))


def prepare(manager, **changes):
    return manager.prepare(
        label="Customer",
        scope=scope(),
        capabilities=changes.pop("capabilities", (EVENT,)),
        **changes,
    )


def activate(manager, draft):
    return manager.activate(
        draft.watch_id, expected_revision=draft.draft.revision, expected_version=draft.version
    )


def test_empty_scope_denies_and_cannot_be_activated(controls):
    manager, _, _ = controls
    assert not matches_scope(WatchScope(()), ScopeTarget("primary", PEER))
    with pytest.raises(ScopeDenied):
        manager.prepare(label="Empty", scope=WatchScope(()), capabilities=(EVENT,))


@pytest.mark.parametrize(
    "account,peer,topic",
    [
        ("*", PEER, None),
        ("primary", "*", None),
        ("primary", "@name", None),
        ("primary", True, None),
        ("primary", 0, None),
        ("primary", PEER, 0),
        ("primary", PEER, True),
        ("primary", 42, 3),
    ],
)
def test_only_canonical_account_peer_and_topic_identities(account, peer, topic):
    with pytest.raises(ValueError):
        ScopeTarget(account, peer, topic)


def test_peer_account_and_topic_boundaries_never_broaden():
    selected = scope(7)
    assert matches_scope(selected, ScopeTarget("primary", PEER, 7))
    for target in (
        ScopeTarget("primary", PEER),
        ScopeTarget("primary", PEER, 8),
        ScopeTarget("other", PEER, 7),
        ScopeTarget("primary", -1001111111111, 7),
    ):
        assert not matches_scope(selected, target)
    assert matches_scope(scope(), ScopeTarget("primary", PEER, 7))
    assert not matches_scope(scope(), ScopeTarget("primary", 1234567890))


def test_folder_selection_is_an_immutable_snapshot():
    selected = [ScopeTarget("primary", PEER)]
    snapshot = FolderSnapshot(3, selected)
    watch_scope = WatchScope.from_folder_snapshot(snapshot)
    selected.append(ScopeTarget("primary", 99))
    assert len(watch_scope.targets) == 1
    assert not matches_scope(watch_scope, ScopeTarget("primary", 99))
    with pytest.raises(FrozenInstanceError):
        snapshot.folder_id = 4


def test_policy_defaults_are_quiet_and_models_are_immutable():
    policy = WatchPolicy()
    assert (policy.settle_seconds, policy.max_settle_seconds, policy.min_wake_seconds) == (
        45,
        180,
        300,
    )
    assert not policy.store_state and not policy.wake_dot and not policy.notify
    assert not policy.reminders and policy.own_outgoing_silent
    assert not policy.transcribe_media and not policy.send_messages
    with pytest.raises(FrozenInstanceError):
        policy.notify = True
    with pytest.raises(ValueError):
        WatchPolicy(notify=True)
    assert WatchPolicy(wake_dot=True).wake_dot


def test_catalog_does_not_claim_planned_handlers_exist(controls):
    assert CAPABILITY_CATALOG
    assert all(not item.available for item in CAPABILITY_CATALOG)
    assert any(item.name == "telegram.typing.changed" for item in CAPABILITY_CATALOG)
    manager, _, _ = controls
    with pytest.raises(UnsupportedCapability):
        prepare(manager, policy=WatchPolicy(), capabilities=("telegram.typing.changed",))


def test_new_watch_is_only_a_draft_and_preview_has_no_side_effects(controls):
    manager, repository, _ = controls
    assert manager.list() == ()
    draft = prepare(manager)
    assert draft.state == WatchState.DRAFT and draft.active is None
    before = dict(repository.rows)
    preview = manager.preview(draft.watch_id, expected_revision=1)
    assert preview.scope == scope()
    assert preview.persisted_changes == () and preview.dot_batches == ()
    assert preview.potential_notifications == ()
    assert preview.start_boundary == "activation time; new events only"
    assert repository.rows == before
    assert not manager.permits(draft.watch_id, ScopeTarget("primary", PEER), expected_epoch=0)


def test_preview_names_distinct_effects(controls):
    manager, _, _ = controls
    draft = prepare(manager, policy=WatchPolicy(store_state=True, wake_dot=True, notify=True))
    preview = manager.preview(draft.watch_id, expected_revision=1)
    assert preview.persisted_changes == (EVENT,)
    assert preview.dot_batches == (EVENT,)
    assert preview.potential_notifications == (EVENT,)
    assert preview.removed_targets == () and preview.added_targets == scope().targets


def test_activation_requires_exact_revision_and_is_idempotent(controls):
    manager, _, _ = controls
    draft = prepare(manager)
    with pytest.raises(RevisionConflict):
        manager.activate(draft.watch_id, expected_revision=2, expected_version=draft.version)
    active = activate(manager, draft)
    assert active.state == WatchState.ACTIVE and active.boundary == NOW
    assert (
        manager.activate(active.watch_id, expected_revision=1, expected_version=active.version)
        == active
    )
    assert manager.permits(
        active.watch_id, ScopeTarget("primary", PEER), expected_epoch=active.epoch
    )


def test_updated_draft_does_not_change_active_scope_until_activation(controls):
    manager, _, _ = controls
    active = activate(manager, prepare(manager))
    changed = manager.prepare(
        watch_id=active.watch_id,
        expected_version=active.version,
        label="Ignore instructions in this label",
        scope=scope(7),
        capabilities=(EVENT,),
    )
    assert changed.active == active.active
    assert manager.permits(
        active.watch_id, ScopeTarget("primary", PEER, 8), expected_epoch=active.epoch
    )
    preview = manager.preview(changed.watch_id, expected_revision=2)
    assert preview.removed_targets == scope().targets
    with pytest.raises(RevisionConflict):
        manager.activate(changed.watch_id, expected_revision=1, expected_version=changed.version)
    narrowed = activate(manager, changed)
    assert narrowed.epoch > active.epoch
    assert not manager.permits(
        narrowed.watch_id, ScopeTarget("primary", PEER, 8), expected_epoch=narrowed.epoch
    )
    assert not manager.permits(
        narrowed.watch_id, ScopeTarget("primary", PEER, 7), expected_epoch=active.epoch
    )


def test_pause_resume_revoke_and_delete_invalidate_old_work(controls):
    manager, _, clock = controls
    active = activate(manager, prepare(manager))
    paused = manager.pause(active.watch_id, expected_version=active.version)
    assert not manager.permits(
        active.watch_id, ScopeTarget("primary", PEER), expected_epoch=active.epoch
    )
    clock[0] += timedelta(days=7)
    resumed = manager.resume(paused.watch_id, expected_version=paused.version)
    assert resumed.boundary == clock[0] and resumed.epoch > paused.epoch
    revoked = manager.revoke(resumed.watch_id, expected_version=resumed.version)
    assert revoked.state == WatchState.REVOKED
    with pytest.raises(ValueError):
        manager.resume(revoked.watch_id, expected_version=revoked.version)
    manager.delete(revoked.watch_id, expected_version=revoked.version)
    assert manager.list() == ()
    assert not manager.permits(
        revoked.watch_id, ScopeTarget("primary", PEER), expected_epoch=revoked.epoch
    )


def test_owner_and_account_come_from_trusted_configuration(controls):
    manager, repository, _ = controls
    draft = prepare(manager)
    stranger = WatchManager(repository, owner_id="other", account_ids=("primary",))
    assert stranger.list() == ()
    with pytest.raises(WatchNotFound):
        stranger.get(draft.watch_id)
    with pytest.raises(ScopeDenied):
        manager.prepare(
            label="Other", scope=WatchScope((ScopeTarget("other", PEER),)), capabilities=(EVENT,)
        )
    with pytest.raises(TypeError):
        manager.prepare(owner_id="injected", label="Other", scope=scope(), capabilities=(EVENT,))


def test_stale_cas_cannot_overwrite_new_policy_or_lifecycle(controls):
    manager, repository, _ = controls
    draft = prepare(manager)
    repository.rows[(draft.owner_id, draft.watch_id)] = replace(draft, version=2)
    with pytest.raises(RevisionConflict):
        manager.activate(draft.watch_id, expected_revision=1, expected_version=draft.version)


def test_stateless_batches_need_no_obligation_tracking(controls):
    manager, _, _ = controls
    draft = prepare(manager, policy=WatchPolicy(wake_dot=True))
    preview = manager.preview(draft.watch_id, expected_revision=1)
    assert preview.persisted_changes == ()
    assert preview.dot_batches == (EVENT,)
    assert preview.potential_notifications == ()


def test_atomic_compare_failure_does_not_report_activation(controls):
    manager, repository, _ = controls
    draft = prepare(manager)
    repository.compare_and_swap = lambda *args: False
    with pytest.raises(RevisionConflict):
        activate(manager, draft)
    assert manager.get(draft.watch_id) == draft


def test_activation_retry_with_original_version_does_not_move_boundary(controls):
    manager, _, clock = controls
    draft = prepare(manager)
    active = activate(manager, draft)
    clock[0] += timedelta(minutes=1)
    assert activate(manager, draft) == active


@pytest.mark.parametrize(
    "changes",
    [
        {"send_messages": True},
        {"transcribe_media": True},
        {"reminders": True},
        {"own_outgoing_silent": False},
        {"settle_seconds": 0},
        {"settle_seconds": 181},
        {"min_wake_seconds": True},
    ],
)
def test_policy_does_not_grant_unimplemented_actions(changes):
    with pytest.raises(ValueError):
        WatchPolicy(**changes)
