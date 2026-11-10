from datetime import datetime, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
from telethon.tl import types
from telegram_mcp.event_bridge.ingress import ScopedIngress
from telegram_mcp.event_bridge.reconciliation import bind_ingress, observe_send
from telegram_mcp.event_bridge.store import SQLiteStore
from telegram_mcp.tools import messages
from test_ingress import setup, event, CHAT


@pytest.mark.asyncio
async def test_same_client_send_and_phone_update_have_one_journal_identity(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        cl = AsyncMock()
        bind_ingress(cl, ingress)
        msg = event(out=True).message
        await observe_send(cl, types.PeerChannel(1234567890), msg)
        await ingress.message(event(out=True))
        assert len(store.get_sources("o", "w")) == 1
        assert store.claim_due("o", now=msg.date) == ()
        bind_ingress(cl, None)


@pytest.mark.asyncio
async def test_accepted_unknown_send_marks_gap_without_retry(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        cl = AsyncMock()
        bind_ingress(cl, ingress)
        await observe_send(cl, types.PeerChannel(1234567890), NS())
        assert store.health("o", "w").gap_count == 1
        cl.send_message.assert_not_called()
        bind_ingress(cl, None)


@pytest.mark.asyncio
async def test_persistence_failure_does_not_change_accepted_send(monkeypatch):
    cl = AsyncMock()
    cl.send_message.return_value = event(out=True).message
    monkeypatch.setattr(messages, "get_client", lambda account=None: cl)
    monkeypatch.setattr(
        messages, "resolve_entity", AsyncMock(return_value=types.PeerChannel(1234567890))
    )
    ingress = NS(message=AsyncMock(side_effect=OSError("disk")))
    bind_ingress(cl, ingress)
    result = await messages.send_message(CHAT, "hello", account="test")
    assert "sent successfully" in result
    cl.send_message.assert_awaited_once()
    bind_ingress(cl, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("reply", [False, True])
@pytest.mark.parametrize("mode", ["plain", "rich", "date"])
async def test_public_send_branches_record_one_accepted_message(
    tmp_path, monkeypatch, reply, mode
):
    with SQLiteStore(tmp_path / "db") as store:
        setup(store)
        ingress = ScopedIngress(store, owner_id="o", account_id="a")
        msg = event(out=True).message

        class Client:
            def __init__(self):
                self.accepted = 0

            async def send_message(self, *args, **kwargs):
                self.accepted += 1
                return msg

            async def __call__(self, request):
                self.accepted += 1
                return NS(updates=[NS(message=msg)])

        cl = Client()
        bind_ingress(cl, ingress)
        monkeypatch.setattr(messages, "get_client", lambda account=None: cl)
        monkeypatch.setattr(
            messages, "resolve_entity", AsyncMock(return_value=types.PeerChannel(1234567890))
        )
        monkeypatch.setattr(messages, "account_is_premium", AsyncMock(return_value=True))
        kwargs = {"account": "test"}
        if mode == "rich":
            kwargs["parse_mode"] = "rich"
        if mode == "date":
            kwargs["format_date"] = "13/09 17:00"
        text = "Due 13/09 17:00"
        if reply:
            result = await messages.reply_to_message(CHAT, 5, text, **kwargs)
        else:
            result = await messages.send_message(CHAT, text, **kwargs)
        assert "Error" not in result
        assert cl.accepted == 1
        assert len(store.get_sources("o", "w")) == 1
        assert store.get_sources("o", "w")[0].record.metadata["outgoing"]
        bind_ingress(cl, None)
