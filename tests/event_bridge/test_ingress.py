from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telegram_mcp.event_bridge.models import Capability, ScopeTarget, WatchScope
from telegram_mcp.event_bridge.tools import WatchManager
from telegram_mcp.event_bridge.store import SQLiteStore
from telegram_mcp.event_bridge.ingress import ScopedIngress

NOW = datetime(2026, 11, 9, tzinfo=timezone.utc)
CHAT = -1001234567890


def setup(store, topic=None):
    kinds = ("telegram.message.created", "telegram.message.edited", "telegram.message.deleted")
    manager = WatchManager(
        store,
        owner_id="o",
        account_ids=("a",),
        clock=lambda: NOW,
        id_factory=lambda: "w",
        catalog=tuple(Capability(k, "message", True, "observed", ("batch",)) for k in kinds),
    )
    draft = manager.prepare(
        label="Customer", scope=WatchScope((ScopeTarget("a", CHAT, topic),)), capabilities=kinds
    )
    manager.activate("w", expected_revision=1, expected_version=draft.version)
    return manager


def event(chat=CHAT, topic=None, out=False, mid=1):
    return NS(
        chat_id=chat,
        message=NS(
            id=mid,
            date=NOW,
            edit_date=None,
            out=out,
            sender_id=42,
            reply_to=NS(reply_to_top_id=topic, reply_to_msg_id=topic, forum_topic=bool(topic)),
            grouped_id=None,
            mentioned=False,
            media=None,
            action=None,
        ),
        get_sender=AsyncMock(side_effect=AssertionError("enrichment")),
        get_chat=AsyncMock(side_effect=AssertionError("enrichment")),
    )


@pytest.mark.asyncio
async def test_no_watch_or_unselected_does_not_enrich_or_store(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        assert await ingress.message(event()) == ()
        setup(store)
        for i in range(1000):
            assert await ingress.message(event(chat=123, mid=i + 1)) == ()
        assert store.get_sources("o", "w") == ()


@pytest.mark.asyncio
async def test_topic_scope_edits_outgoing_dedup_and_delete(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store, topic=55)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        assert await ingress.message(event(topic=56)) == ()
        e = event(topic=55, out=True)
        first = await ingress.message(e)
        assert len(first) == 1 and first[0].record.metadata["outgoing"]
        assert await ingress.message(e, own_send=True) == first
        e.message.edit_date = NOW
        assert len(await ingress.message(e, edited=True)) == 1
        assert await ingress.deleted(NS(chat_id=None, deleted_ids=[1]))
        assert len(store.get_sources("o", "w")) == 3


@pytest.mark.asyncio
async def test_two_edits_in_same_second_keep_distinct_update_versions(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        e = event()
        e.message.edit_date = NOW
        e.original_update = NS(pts=10)
        first = await ingress.message(e, edited=True)
        e.original_update = NS(pts=11)
        second = await ingress.message(e, edited=True)
        assert first[0].source_id != second[0].source_id
        assert len(store.get_sources("o", "w")) == 2


@pytest.mark.asyncio
async def test_backfill_is_selected_bounded_and_reports_incomplete(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")

        class Client:
            async def iter_messages(self, chat_id, **kwargs):
                assert chat_id == CHAT
                assert kwargs["limit"] == 3
                yield event(mid=1).message
                yield event(mid=2).message
                yield event(mid=3).message

        assert await ingress.backfill(Client(), "w", ScopeTarget("a", CHAT), limit=2) == 2
        assert len(store.get_sources("o", "w")) == 2
        assert store.health("o", "w").gap_count == 1
        from telegram_mcp.event_bridge.scope import ScopeDenied

        with pytest.raises(ScopeDenied):
            await ingress.backfill(Client(), "w", ScopeTarget("a", 123))


@pytest.mark.asyncio
async def test_peerless_delete_ambiguity_is_not_exported(tmp_path):
    from dataclasses import replace

    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        await ingress.message(event())
        original = store.get("o", "w")
        second = replace(
            original,
            watch_id="second",
            active=replace(original.active, scope=WatchScope((ScopeTarget("a", 123),))),
        )
        assert store.compare_and_swap("o", "second", None, second)
        await ingress.message(event(chat=123))
        assert await ingress.deleted(NS(chat_id=None, deleted_ids=[1])) == ()
        assert len(store.get_sources("o", "w")) == 1


@pytest.mark.asyncio
async def test_backfill_stops_before_activation_and_access_loss_fences_epoch(tmp_path):
    from datetime import timedelta
    from telegram_mcp.event_bridge.models import WatchState

    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")

        class OldHistory:
            async def iter_messages(self, *args, **kwargs):
                e = event()
                e.message.date = NOW - timedelta(days=1)
                yield e.message

        assert await ingress.backfill(OldHistory(), "w", ScopeTarget("a", CHAT)) == 0
        assert store.get_sources("o", "w") == ()
        before = store.get("o", "w")
        ingress.access_lost(CHAT)
        after = store.get("o", "w")
        assert after.state == WatchState.PAUSED and after.epoch > before.epoch
        assert await ingress.message(event()) == ()
