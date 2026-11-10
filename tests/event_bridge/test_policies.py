from datetime import timedelta
from dataclasses import replace

from telegram_mcp.event_bridge.policies import PolicyScheduler, AnalysisResult, apply_analysis
from telegram_mcp.event_bridge.store import SQLiteStore, DeliveryRequest
from test_store import active, record, NOW


def test_burst_settles_once_and_outgoing_alone_stays_silent(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        _, w = active(store)
        now = [NOW]
        scheduler = PolicyScheduler(store, owner_id="owner", clock=lambda: now[0])
        req = (DeliveryRequest("r", "s"),)
        for mid in range(1, 11):
            store.append_source("owner", "watch", w.epoch, record(mid))
        assert scheduler.flush("watch", req) is None
        now[0] += timedelta(seconds=46)
        assert scheduler.flush("watch", req)
        assert len(store.claim_due("owner", now=now[0], limit=100)) == 1
        store.append_source(
            "owner", "watch", w.epoch, replace(record(11, outgoing=True), occurred_at=now[0])
        )
        now[0] += timedelta(minutes=10)
        assert scheduler.flush("watch", req) is None


def test_restart_budget_keeps_one_fresh_digest_and_overlap_dedup(tmp_path):
    path = tmp_path / "db"
    now = [NOW + timedelta(seconds=46)]
    req = (DeliveryRequest("r", "s"),)
    with SQLiteStore(path) as store:
        _, w = active(store)
        _, other = active(store, watch_id="other")
        for watch in (w, other):
            store.append_source("owner", watch.watch_id, watch.epoch, record())
        s = PolicyScheduler(store, owner_id="owner", clock=lambda: now[0])
        assert s.flush("watch", req)
        assert s.flush("other", (DeliveryRequest("r", "s2"),)) is None
    with SQLiteStore(path) as store:
        s = PolicyScheduler(store, owner_id="owner", clock=lambda: now[0])
        assert s.flush("watch", req) is None
        for mid in range(2, 5):
            now[0] += timedelta(minutes=6)
            store.append_source(
                "owner",
                "watch",
                w.epoch,
                replace(record(mid), occurred_at=now[0] - timedelta(seconds=46)),
            )
            result = s.flush("watch", req)
            assert bool(result) == (mid < 4)
        now[0] = NOW + timedelta(hours=1, minutes=1)
        assert s.flush("watch", req)


def test_analysis_requires_evidence_and_does_not_close_other_questions(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        _, w = active(store)
        source = store.append_source("owner", "watch", w.epoch, record())
        snap = store.analysis_snapshot("owner", "watch", w.epoch, ("one", "two", "three"))
        results = tuple(
            AnalysisResult(i, "awaiting_me", "Question " + i, (source.sequence,), 0.95)
            for i in ("one", "two", "three")
        )
        assert apply_analysis(store, snap, results)
        # A structural outgoing candidate alone makes no semantic transition.
        store.append_source("owner", "watch", w.epoch, record(2, outgoing=True))
        assert {o.state for o in store.get_obligations("owner", "watch")} == {"awaiting_me"}
        fresh = store.analysis_snapshot("owner", "watch", w.epoch, ("one",))
        assert apply_analysis(
            store, fresh, (AnalysisResult("one", "done", "Answered one", (1, 2), 0.95),)
        )
        assert [o.state for o in store.get_obligations("owner", "watch")].count("done") == 1
        assert not apply_analysis(store, snap, results)
        low = store.analysis_snapshot("owner", "watch", w.epoch, ("two",))
        assert apply_analysis(
            store, low, (AnalysisResult("two", "done", "Uncertain", (1, 2), 0.3),)
        )
        assert (
            next(
                o for o in store.get_obligations("owner", "watch") if o.obligation_id == "two"
            ).state
            == "needs_review"
        )


def test_quiet_backlog_and_continuous_stream_have_bounded_fresh_packets(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        manager, w = active(store)
        quiet = replace(
            w.active.policy, quiet_start_minute=0, quiet_end_minute=240, quiet_timezone="UTC"
        )
        draft = manager.prepare(
            watch_id="watch",
            expected_version=w.version,
            label="quiet",
            scope=w.active.scope,
            capabilities=w.active.capabilities,
            policy=quiet,
        )
        w = manager.activate(
            "watch", expected_revision=draft.draft.revision, expected_version=draft.version
        )
        now = [NOW + timedelta(hours=1)]
        s = PolicyScheduler(store, owner_id="owner", clock=lambda: now[0])
        req = (DeliveryRequest("r", "s"),)
        for mid in range(1, 11):
            store.append_source(
                "owner",
                "watch",
                w.epoch,
                replace(record(mid), occurred_at=now[0] - timedelta(seconds=46)),
            )
        assert s.flush("watch", req) is None
        now[0] = NOW + timedelta(hours=4)
        assert s.flush("watch", req)
        assert s.flush("watch", req) is None
        now[0] += timedelta(minutes=10)
        store.append_source(
            "owner",
            "watch",
            w.epoch,
            replace(record(11), occurred_at=now[0] - timedelta(seconds=181)),
        )
        store.append_source("owner", "watch", w.epoch, replace(record(12), occurred_at=now[0]))
        assert s.flush("watch", req)


def test_explicit_reminder_recheck_fences_answer_pause_and_deadline_change(tmp_path):
    from telegram_mcp.event_bridge.policies import ReminderPolicy

    with SQLiteStore(tmp_path / "db") as store:
        manager, w = active(store)
        src = store.append_source("owner", "watch", w.epoch, record())
        snap = store.analysis_snapshot("owner", "watch", w.epoch, ("one",))
        assert apply_analysis(
            store, snap, (AnalysisResult("one", "awaiting_me", "Reply", (src.sequence,), 0.95),)
        )
        now = [NOW + timedelta(hours=1)]
        reminders = ReminderPolicy(store, owner_id="owner", clock=lambda: now[0])
        assert reminders.due("watch") == ()
        reminders.schedule("watch", "one", expected_version=1, due_at=NOW + timedelta(minutes=30))
        assert len(reminders.due("watch")) == 1
        store.append_source("owner", "watch", w.epoch, record(2, outgoing=True))
        assert reminders.due("watch") == ()
        # Reconfirm only after reading the fresh outgoing; a later deadline replaces it.
        reminders.schedule("watch", "one", expected_version=1, due_at=NOW + timedelta(hours=2))
        assert reminders.due("watch") == ()
        now[0] += timedelta(hours=2)
        assert len(reminders.due("watch")) == 1
        manager.pause("watch", expected_version=w.version)
        assert reminders.due("watch") == ()


def test_callback_wake_count_cannot_bypass_global_packet_cap(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        manager, w = active(store)
        policy = replace(
            w.active.policy, notifications_per_hour=1, account_notifications_per_hour=1
        )
        draft = manager.prepare(
            watch_id="watch",
            expected_version=w.version,
            label="one packet",
            scope=w.active.scope,
            capabilities=w.active.capabilities,
            policy=policy,
        )
        w = manager.activate(
            "watch", expected_revision=draft.draft.revision, expected_version=draft.version
        )
        store.append_source("owner", "watch", w.epoch, record())
        scheduler = PolicyScheduler(
            store, owner_id="owner", clock=lambda: NOW + timedelta(seconds=46)
        )
        assert (
            scheduler.flush("watch", (DeliveryRequest("one", "s1"), DeliveryRequest("two", "s2")))
            is None
        )
        assert store.claim_due("owner", now=NOW + timedelta(seconds=46)) == ()


def test_overlap_mixed_packet_only_contains_recipient_unseen_sources(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        _, one = active(store)
        _, two = active(store, watch_id="other")
        now = NOW + timedelta(seconds=46)
        scheduler = PolicyScheduler(store, owner_id="owner", clock=lambda: now)
        store.append_source("owner", "watch", one.epoch, record())
        scheduler.flush("watch", (DeliveryRequest("r", "s"),))
        for mid in (1, 2):
            store.append_source("owner", "other", two.epoch, record(mid))
        scheduler.flush("other", (DeliveryRequest("r", "s2"),))
        deliveries = store.claim_due("owner", now=now, limit=10)
        packet = next(d for d in deliveries if d.watch_id == "other")
        sources = store.get_sources("owner", "other")
        assert packet.payload["source_ids"] == (sources[1].source_id,)
