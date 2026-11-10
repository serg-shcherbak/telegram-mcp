"""Hermetic composition: native update, webhook, scoped analysis and revocation."""

import base64
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
from mcp.server.fastmcp import FastMCP
import pytest
from telethon import types

from telegram_mcp.event_bridge.delivery import DeliveryWorker
from telegram_mcp.event_bridge.models import Capability, ScopeTarget, WatchPolicy, WatchScope
from telegram_mcp.event_bridge.protocol import ACTIVITY
from telegram_mcp.event_bridge.runtime import configure
from telegram_mcp.event_bridge.store import SQLiteStore
from telegram_mcp.event_bridge.tools import WatchManager
from test_delivery import Receiver
from test_lifecycle import Client, files


@pytest.mark.asyncio
async def test_selected_update_reaches_scoped_analysis_and_pause_stops_processing(tmp_path):
    env = files(tmp_path)
    env.update(DOT_EVENTS_DELIVERY_ENABLED="1", DOT_EVENTS_CONTEXT_VERIFIED="1")
    clock = [datetime.now(timezone.utc)]
    path = Path(env["DOT_EVENTS_DB"])
    path.touch(mode=0o600)
    with SQLiteStore(path) as seed:
        controls = WatchManager(
            seed,
            owner_id="123",
            account_ids=("123",),
            id_factory=lambda: "watch",
            clock=lambda: clock[0],
            catalog=(Capability("telegram.message.created", "B", True, "fixture", ("batch",)),),
        )
        draft = controls.prepare(
            label="Customer",
            scope=WatchScope((ScopeTarget("123", 42),)),
            capabilities=("telegram.message.created",),
            policy=WatchPolicy(store_state=True, wake_dot=True),
        )
        controls.activate("watch", expected_revision=1, expected_version=draft.version)

    incoming = types.Message(
        id=1, peer_id=types.PeerUser(42), date=clock[0], message="Когда будет оценка?"
    )

    class ExistingClient(Client):
        async def get_messages(self, peer, *, ids):
            assert peer == 42 and ids == [1]
            return [incoming]

    client = ExistingClient()
    app = configure(FastMCP("manual", stateless_http=True, json_response=True), client, env=env)
    service = app.apps["manual"].service
    service.clock = lambda: clock[0]
    receiver = Receiver()
    service.callbacks = receiver

    async with app.lifespan(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8765"
        ) as http:

            async def rpc(method, params, token="c" * 32):
                response = await http.post(
                    "/mcp",
                    headers={
                        "Authorization": "Bearer " + token,
                        "Accept": "application/json, text/event-stream",
                    },
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                )
                result = response.json()
                assert response.status_code == 200 and "error" not in result, result
                return result["result"]

            async def tool(name, arguments, token="c" * 32):
                result = await rpc("tools/call", {"name": name, "arguments": arguments}, token)
                assert not result.get("isError"), result
                return json.loads(result["content"][0]["text"])

            await rpc(
                "events/subscribe",
                {
                    "name": ACTIVITY,
                    "arguments": {"watch_id": "watch", "scope_version": 1},
                    "delivery": {
                        "mode": "webhook",
                        "url": "https://receiver.example/events",
                        "secret": "whsec_" + base64.b64encode(b"k" * 32).decode(),
                    },
                },
            )
            await client.handlers[0](SimpleNamespace(chat_id=99, message=incoming))
            assert service.store.get_sources("123", "watch") == ()
            await client.handlers[0](SimpleNamespace(chat_id=42, message=incoming))
            clock[0] += timedelta(seconds=46)
            await DeliveryWorker(service, owner_id="123").run_once()
            assert len(receiver.posts) == 1
            event = json.loads(receiver.posts[0][1])
            assert event["name"] == ACTIVITY and event["data"]["watch_id"] == "watch"
            assert service.store.get_obligations("123", "watch") == ()
            job = await tool(
                "begin_analysis",
                {
                    "watch_id": "watch",
                    "source_sequences": event["data"]["sequences"],
                    "obligation_ids": ["estimate"],
                },
            )
            assert job["context"]["messages"][0]["text"] == incoming.message
            receipt = await tool(
                "commit_analysis",
                {
                    "job_id": job["job_id"],
                    "results": [
                        {
                            "obligation_id": "estimate",
                            "state": "awaiting_me",
                            "summary": "Ответить об оценке",
                            "evidence": event["data"]["sequences"],
                            "confidence": 0.9,
                        }
                    ],
                },
            )
            assert receipt["analysis_completed"] and receipt["current"]
            assert service.store.get_obligations("123", "watch")[0].state == "awaiting_me"
            watch = await tool("watch_get", {"watch_id": "watch"}, "m" * 32)
            await tool(
                "watch_pause",
                {"watch_id": "watch", "expected_version": watch["version"]},
                "m" * 32,
            )
            await client.handlers[0](
                SimpleNamespace(
                    chat_id=42,
                    message=types.Message(
                        id=2, peer_id=types.PeerUser(42), date=clock[0], message="Ещё вопрос"
                    ),
                )
            )
            assert len(service.store.get_sources("123", "watch")) == 1
            await DeliveryWorker(service, owner_id="123").run_once()
            assert len(receiver.posts) == 1
    assert client.handlers == []
