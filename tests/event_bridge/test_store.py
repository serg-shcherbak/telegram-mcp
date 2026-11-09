"""Real SQLite behavioral checks for durable, owner-scoped event state."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from telegram_mcp.event_bridge.models import ScopeTarget, WatchPolicy, WatchScope
from telegram_mcp.event_bridge.scope import ScopeDenied
from telegram_mcp.event_bridge.store import (
    DeliveryRequest,
    ObligationChange,
    SQLiteStore,
    SourceRecord,
    StoreLimits,
)
from telegram_mcp.event_bridge.tools import WatchManager

NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)
TARGET = ScopeTarget("account", -1001234567890)
KIND = "telegram.message.created"


def active(store, owner="owner", watch_id="watch"):
    from telegram_mcp.event_bridge.models import Capability

    manager = WatchManager(
        store,
        owner_id=owner,
        account_ids=("account",),
        catalog=(Capability(KIND, "message", True, "observed", ("batch",)),),
        id_factory=lambda: watch_id,
        clock=lambda: NOW,
    )
    draft = manager.prepare(
        label="Client",
        scope=WatchScope((TARGET,)),
        capabilities=(KIND,),
        policy=WatchPolicy(store_state=True, wake_dot=True),
    )
    return manager, manager.activate(watch_id, expected_revision=1, expected_version=draft.version)


def record(message_id=1, version="1", **metadata):
    return SourceRecord(TARGET, KIND, message_id, NOW, version=version, metadata=metadata)


def test_restart_owner_isolation_frozen_watch_and_cas(tmp_path):
    path = tmp_path / "events.sqlite"
    with SQLiteStore(path) as store:
        _, watch = active(store)
        assert not store.compare_and_swap("owner", "watch", 1, watch)
        assert store.get("other", "watch") is None
        assert store.list("other") == ()
        assert not store.compare_and_swap("other", "watch", None, watch)
    with SQLiteStore(path) as store:
        assert store.get("owner", "watch") == watch
        assert store.get("owner", "watch").boundary.utcoffset() is not None
        assert store.journal_mode == "delete"


def test_source_dedup_edits_checkpoint_restart_and_atomic_outbox(tmp_path):
    path = tmp_path / "events.sqlite"
    request = DeliveryRequest("recipient", "subscription")
    with SQLiteStore(path) as store:
        _, watch = active(store)
        first = store.append_source("owner", "watch", watch.epoch, record(), deliveries=(request,))
        assert (
            store.append_source("owner", "watch", watch.epoch, record(), deliveries=(request,))
            == first
        )
        edited = store.append_source("owner", "watch", watch.epoch, record(version="2"))
        assert edited.source_id != first.source_id
        assert store.set_checkpoint("owner", "watch", watch.epoch, TARGET, "updates", "42")
    with SQLiteStore(path) as store:
        assert len(store.get_sources("owner", "watch")) == 2
        assert store.get_sources("other", "watch") == ()
        assert store.get_checkpoint("owner", "watch", TARGET, "updates") == "42"
        delivery = store.claim_due("owner", now=NOW)[0]
        assert delivery.event_id == first.event_id
        assert delivery.payload["source_ids"] == (first.source_id,)


def test_scope_epoch_and_metadata_denied_before_persistence(tmp_path):
    with SQLiteStore(tmp_path / "events.sqlite") as store:
        manager, watch = active(store)
        for owner, epoch, value in [
            ("other", watch.epoch, record()),
            ("owner", watch.epoch + 1, record()),
            ("owner", watch.epoch, replace(record(), target=ScopeTarget("account", 123))),
        ]:
            with pytest.raises(ScopeDenied):
                store.append_source(owner, "watch", epoch, value)
        with pytest.raises(ValueError):
            store.append_source("owner", "watch", watch.epoch, record(body="secret"))
        manager.revoke("watch", expected_version=watch.version)
        with pytest.raises(ScopeDenied):
            store.append_source("owner", "watch", watch.epoch, record())
        assert store.get_sources("owner", "watch") == ()


def test_epoch_change_cancels_pending_and_claimed_but_draft_does_not(tmp_path):
    with SQLiteStore(tmp_path / "events.sqlite") as store:
        manager, watch = active(store)
        store.append_source(
            "owner", "watch", watch.epoch, record(), deliveries=(DeliveryRequest("r", "s"),)
        )
        draft = manager.prepare(
            label="New label",
            scope=watch.active.scope,
            capabilities=(KIND,),
            watch_id="watch",
            expected_version=watch.version,
        )
        delivery = store.claim_due("owner", now=NOW)[0]
        assert store.delivery_current("owner", delivery.delivery_id, delivery.claim_token)
        manager.pause("watch", expected_version=draft.version)
        assert not store.delivery_current("owner", delivery.delivery_id, delivery.claim_token)
        assert not store.ack("owner", delivery.delivery_id, delivery.claim_token)
        assert store.get_delivery("owner", delivery.delivery_id).state == "cancelled"
        assert store.claim_due("owner", now=NOW + timedelta(hours=1)) == ()


def test_ack_crash_retries_same_event_and_excludes_stale_lease(tmp_path):
    path = tmp_path / "events.sqlite"
    with SQLiteStore(path) as store:
        _, watch = active(store)
        stored = store.append_source(
            "owner", "watch", watch.epoch, record(), deliveries=(DeliveryRequest("r", "s"),)
        )
        first = store.claim_due("owner", now=NOW, lease_seconds=10)[0]
    with SQLiteStore(path) as store:
        assert store.claim_due("owner", now=NOW + timedelta(seconds=9)) == ()
        repeated = store.claim_due("owner", now=NOW + timedelta(seconds=11))[0]
        assert repeated.event_id == stored.event_id == first.event_id
        assert repeated.delivery_id == first.delivery_id
        assert repeated.claim_token != first.claim_token
        assert not store.ack("owner", first.delivery_id, first.claim_token)
        assert store.ack("owner", repeated.delivery_id, repeated.claim_token)
        assert store.claim_due("owner", now=NOW + timedelta(days=1)) == ()


def test_resource_overflow_is_visible_persistent_and_does_not_evict(tmp_path):
    path = tmp_path / "events.sqlite"
    with SQLiteStore(path, limits=StoreLimits(max_sources=1)) as store:
        _, watch = active(store)
        first = store.append_source("owner", "watch", watch.epoch, record())
        assert store.append_source("owner", "watch", watch.epoch, record(2)) is None
        assert store.health("owner", "watch").gap_count == 1
    with SQLiteStore(path) as store:
        assert store.get_sources("owner", "watch") == (first,)
        assert store.health("owner", "watch").status == "degraded"


def test_semantic_commit_rechecks_watermark_epoch_and_obligation_versions(tmp_path):
    with SQLiteStore(tmp_path / "events.sqlite") as store:
        manager, watch = active(store)
        store.append_source("owner", "watch", watch.epoch, record())
        snap = store.analysis_snapshot("owner", "watch", watch.epoch, ("obligation",))
        change = ObligationChange("obligation", "awaiting_me", "Reply requested", (1,))
        store.append_source("owner", "watch", watch.epoch, record(2))
        assert not store.commit_analysis(snap, (change,))
        snap = store.analysis_snapshot("owner", "watch", watch.epoch, ("obligation",))
        assert store.commit_analysis(snap, (change,))
        assert not store.commit_analysis(snap, (replace(change, state="done"),))
        newer = store.analysis_snapshot("owner", "watch", watch.epoch, ("obligation",))
        assert store.get_obligations("owner", "watch")[0].version == 1
        manager.pause("watch", expected_version=watch.version)
        assert not store.commit_analysis(newer, (change,))


def test_budget_restart_retention_and_revoked_delete_cascade(tmp_path):
    path = tmp_path / "events.sqlite"
    with SQLiteStore(path) as store:
        manager, watch = active(store)
        assert store.reserve_budget("owner", "watch", watch.epoch, "wake", NOW, 1, limit=1)
        stored = store.append_source(
            "owner", "watch", watch.epoch, record(), deliveries=(DeliveryRequest("r", "s"),)
        )
    with SQLiteStore(path) as store:
        assert not store.reserve_budget("owner", "watch", watch.epoch, "wake", NOW, 1, limit=1)
        delivery = store.claim_due("owner", now=NOW)[0]
        assert delivery.event_id == stored.event_id
        assert store.get_delivery("owner", delivery.delivery_id) is not None
        assert store.prune("owner", before=NOW + timedelta(days=4)) == 1
        assert store.get_sources("owner", "watch") == ()
        assert store.health("owner", "watch").retention_floor > 0
        assert store.get_delivery("owner", delivery.delivery_id) is None
        manager = WatchManager(store, owner_id="owner", account_ids=("account",))
        revoked = manager.revoke("watch", expected_version=watch.version)
        manager.delete("watch", expected_version=revoked.version)
        assert store.get("owner", "watch") is None
        assert store.health("owner", "watch") is None
        assert store.get_obligations("owner", "watch") == ()


def test_source_and_outbox_rollback_together_on_storage_error(tmp_path):
    import sqlite3
    from telegram_mcp.event_bridge.store import StoreUnavailable

    path = tmp_path / "events.sqlite"
    with SQLiteStore(path) as store:
        _, watch = active(store)
        with sqlite3.connect(path) as db:
            db.execute(
                "CREATE TRIGGER refuse_delivery BEFORE INSERT ON deliveries BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
            )
        with pytest.raises(StoreUnavailable):
            store.append_source(
                "owner", "watch", watch.epoch, record(), deliveries=(DeliveryRequest("r", "s"),)
            )
        assert store.get_sources("owner", "watch") == ()
        assert store.claim_due("owner", now=NOW) == ()
        assert store.analysis_snapshot("owner", "watch", watch.epoch).watermark == 0
        assert store.health("owner", "watch").status == "degraded"
    with SQLiteStore(path) as store:
        assert store.health("owner", "watch").reason == "storage_failure"


def test_second_precision_boundary_and_immutable_source_record(tmp_path):
    with SQLiteStore(tmp_path / "events.sqlite") as store:
        _, watch = active(store)
        later_boundary = replace(
            watch, version=watch.version + 1, boundary=NOW + timedelta(microseconds=500000)
        )
        assert store.compare_and_swap("owner", "watch", watch.version, later_boundary)
        first = store.append_source("owner", "watch", watch.epoch, record(outgoing=False))
        assert store.append_source("owner", "watch", watch.epoch, record(outgoing=True)) == first
        with pytest.raises(ScopeDenied):
            store.append_source(
                "owner",
                "watch",
                watch.epoch,
                replace(record(2), occurred_at=NOW - timedelta(seconds=1)),
            )
        assert first.record.metadata["outgoing"] is False
        with pytest.raises(TypeError):
            first.record.metadata["outgoing"] = True


def test_byte_budget_overflow_batch_retry_and_recipient_isolation(tmp_path):
    path = tmp_path / "events.sqlite"
    with SQLiteStore(path, limits=StoreLimits(max_payload_bytes=1)) as store:
        _, watch = active(store)
        assert store.append_source("owner", "watch", watch.epoch, record()) is None
        assert store.health("owner", "watch").reason == "storage_limit"
    with SQLiteStore(path) as store:
        watch = store.get("owner", "watch")
        source = store.append_source("owner", "watch", watch.epoch, record())
        event = store.enqueue_batch(
            "owner",
            "watch",
            watch.epoch,
            (source.sequence,),
            deliveries=(DeliveryRequest("r", "s"), DeliveryRequest("r2", "s2")),
            now=NOW,
        )
        assert (
            store.enqueue_batch(
                "owner",
                "watch",
                watch.epoch,
                (source.sequence,),
                deliveries=(DeliveryRequest("r", "s"),),
                now=NOW,
            )
            == event
        )
        deliveries = store.claim_due("owner", now=NOW, limit=10)
        assert len(deliveries) == 2
        assert store.get_delivery("other", deliveries[0].delivery_id) is None
        assert not store.ack("other", deliveries[0].delivery_id, deliveries[0].claim_token)
        assert store.retry(
            "owner",
            deliveries[0].delivery_id,
            deliveries[0].claim_token,
            due_at=NOW + timedelta(seconds=30),
        )
        assert store.claim_due("owner", now=NOW + timedelta(seconds=29)) == ()
        repeated = store.claim_due("owner", now=NOW + timedelta(seconds=30))[0]
        assert repeated.event_id == event
        assert store.retry(
            "owner", repeated.delivery_id, repeated.claim_token, due_at=NOW, dead=True
        )
        assert store.get_delivery("owner", repeated.delivery_id).state == "dead"


def test_two_connections_compare_and_swap_one_winner(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    path = tmp_path / "events.sqlite"
    with SQLiteStore(path) as one, SQLiteStore(path) as two:
        _, watch = active(one)
        changed = replace(watch, version=watch.version + 1)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(
                pool.map(
                    lambda store: store.compare_and_swap("owner", "watch", watch.version, changed),
                    (one, two),
                )
            )
        assert sorted(results) == [False, True]
        assert one.get("owner", "watch") == two.get("owner", "watch") == changed


@pytest.mark.parametrize(
    "metadata",
    [
        {"sender_bot": "yes"},
        {"reactions": {"text": "private body"}},
        {"deleted_ids": ["private body"]},
        {"media_type": "full message text"},
        {"chat_type": "wildcard"},
    ],
)
def test_metadata_fields_have_specific_bounded_schemas(metadata):
    with pytest.raises(ValueError):
        record(**metadata)
