import json
from datetime import timedelta
import pytest
from cryptography.fernet import Fernet
from telegram_mcp.event_bridge.protocol import EventService
from telegram_mcp.event_bridge.delivery import DeliveryWorker
from telegram_mcp.event_bridge.callbacks import CallbackResponse
from telegram_mcp.event_bridge.store import SQLiteStore
from test_protocol import Callbacks, params
from test_store import active, NOW, record


class Receiver(Callbacks):
    def __init__(self, status=202):
        super().__init__()
        self.status = status
        self.posts = []

    async def post(self, url, body, headers):
        self.posts.append((url, body, headers))
        return CallbackResponse(self.status, {}, b"")


@pytest.mark.asyncio
async def test_delivery_ack_is_not_analysis_and_retry_has_stable_id(tmp_path):
    now = [NOW]
    receiver = Receiver(503)
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        await service.subscribe("owner", params(watch))
        store.append_source("owner", "watch", watch.epoch, record())
        now[0] += timedelta(seconds=46)
        worker = DeliveryWorker(service, owner_id="owner")
        await worker.run_once()
        assert len(receiver.posts) == 1
        first = json.loads(receiver.posts[0][1])
        assert set(first) == {"eventId", "name", "timestamp", "data", "cursor"}
        now[0] += timedelta(minutes=6)
        receiver.status = 202
        await worker.run_once()
        assert len(receiver.posts) == 2
        assert first == json.loads(receiver.posts[1][1])
        assert (
            receiver.posts[0][2]["webhook-signature"] != receiver.posts[1][2]["webhook-signature"]
        )
        assert store.get_obligations("owner", "watch") == ()
        assert not store.claim_due("owner", now=now[0] + timedelta(hours=1))


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [410, 413])
async def test_terminal_responses_do_not_retry(tmp_path, status):
    now = [NOW + timedelta(seconds=46)]
    receiver = Receiver(status)
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        await service.subscribe("owner", params(watch))
        store.append_source("owner", "watch", watch.epoch, record())
        worker = DeliveryWorker(service, owner_id="owner")
        await worker.run_once()
        now[0] += timedelta(hours=1)
        await worker.run_once()
        assert len(receiver.posts) == 1


@pytest.mark.asyncio
async def test_receipt_frontier_survives_retention(tmp_path):
    now = [NOW + timedelta(seconds=46)]
    receiver = Receiver()
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        p = params(watch)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        await service.subscribe("owner", p)
        store.append_source("owner", "watch", watch.epoch, record())
        worker = DeliveryWorker(service, owner_id="owner")
        await worker.run_once()
        cursor = json.loads(receiver.posts[-1][1])["cursor"]
        store.prune("owner", before=now[0] + timedelta(seconds=1))
        result = await service.subscribe("owner", dict(p, cursor=cursor))
        assert result["truncated"]


@pytest.mark.asyncio
async def test_replay_is_bounded_budgeted_and_owner_bound(tmp_path):
    from telegram_mcp.event_bridge.protocol import ProtocolError

    now = [NOW + timedelta(seconds=46)]
    receiver = Receiver()
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        p = params(watch)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        grant_result = await service.subscribe("owner", p)
        store.append_source("owner", "watch", watch.epoch, record())
        worker = DeliveryWorker(service, owner_id="owner")
        await worker.run_once()
        initial = receiver.posts[-1][1]
        await service.subscribe("owner", dict(p, cursor=grant_result["cursor"]))
        await worker.run_once()
        assert len(receiver.posts) == 1
        now[0] += timedelta(minutes=6)
        await worker.run_once()
        assert len(receiver.posts) == 2 and receiver.posts[-1][1] == initial
        _, other = active(store, watch_id="other")
        other_params = params(other)
        other_params["arguments"]["watch_id"] = "other"
        with pytest.raises(ProtocolError):
            await service.subscribe("owner", dict(other_params, cursor=grant_result["cursor"]))


@pytest.mark.asyncio
async def test_crash_after_post_retries_same_event_and_revoke_fences(tmp_path):
    from dataclasses import replace
    from telegram_mcp.event_bridge.models import WatchState

    now = [NOW + timedelta(seconds=46)]
    receiver = Receiver()
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        await service.subscribe("owner", params(watch))
        store.append_source("owner", "watch", watch.epoch, record())
        worker = DeliveryWorker(service, owner_id="owner")
        original = store.ack
        store.ack = lambda *a: False
        await worker.run_once()
        store.ack = original
        now[0] += timedelta(minutes=6)
        await worker.run_once()
        assert len(receiver.posts) == 2 and receiver.posts[0][1] == receiver.posts[1][1]
        store.append_source("owner", "watch", watch.epoch, record(2))
        now[0] += timedelta(minutes=6)
        worker.flush()
        revoked = replace(
            watch, state=WatchState.REVOKED, version=watch.version + 1, epoch=watch.epoch + 1
        )
        assert store.compare_and_swap("owner", "watch", watch.version, revoked)
        await worker.run_once()
        assert len(receiver.posts) == 2


@pytest.mark.asyncio
async def test_control_publications_share_budget_and_stale_rechecks_are_cancelled(tmp_path):
    from dataclasses import replace
    from telegram_mcp.event_bridge.protocol import ATTENTION, HEALTH
    from telegram_mcp.event_bridge.policies import ReminderPolicy, apply_analysis, AnalysisResult

    now = [NOW + timedelta(seconds=46)]
    receiver = Receiver()
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        policy = replace(
            watch.active.policy, notifications_per_hour=1, account_notifications_per_hour=1
        )
        changed = replace(
            watch, version=watch.version + 1, active=replace(watch.active, policy=policy)
        )
        assert store.compare_and_swap("owner", "watch", watch.version, changed)
        service = EventService(
            store,
            key=Fernet.generate_key(),
            callbacks=receiver,
            clock=lambda: now[0],
            available=(ATTENTION, HEALTH),
        )
        for name in (ATTENTION, HEALTH):
            await service.subscribe("owner", dict(params(watch), name=name))
        source = store.append_source("owner", "watch", watch.epoch, record())
        snapshot = store.analysis_snapshot("owner", "watch", watch.epoch, ("one",))
        assert apply_analysis(
            store,
            snapshot,
            (AnalysisResult("one", "awaiting_me", "Question", (source.sequence,), 0.99),),
        )
        reminders = ReminderPolicy(store, owner_id="owner", clock=lambda: now[0])
        reminders.schedule("watch", "one", expected_version=1, due_at=now[0])
        worker = DeliveryWorker(service, owner_id="owner")
        worker.flush()
        # Source mutation after enqueue invalidates the reminder snapshot before POST.
        store.append_source("owner", "watch", watch.epoch, record(2))
        await worker.run_once()
        assert receiver.posts == []
        now[0] += timedelta(minutes=6)
        await worker.run_once()
        assert receiver.posts == []  # Cancelled work never refunds the shared hourly cap.
        now[0] += timedelta(hours=1)
        await worker.run_once()
        assert len(receiver.posts) == 1
        assert json.loads(receiver.posts[0][1])["name"] == HEALTH
        now[0] += timedelta(hours=1)
        await worker.run_once()
        assert len(receiver.posts) == 1  # Same safe status coalesces durably.
