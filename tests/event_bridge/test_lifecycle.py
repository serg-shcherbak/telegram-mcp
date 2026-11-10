import asyncio
import json
import os
import pytest
import httpx
from mcp.server.fastmcp import FastMCP
from cryptography.fernet import Fernet
from telegram_mcp.event_bridge.runtime import configure, load_profiles


def files(tmp_path):
    private = tmp_path / "events"
    private.mkdir(mode=0o700)
    profiles = tmp_path / "profiles.json"
    profiles.write_text(
        json.dumps(
            {
                "owner_id": "123",
                "profiles": [
                    {"id": "manual", "kind": "manual", "token": "m" * 32},
                    {
                        "id": "context",
                        "kind": "context",
                        "token": "c" * 32,
                        "watch_ids": ["watch"],
                    },
                ],
            }
        )
    )
    profiles.chmod(0o600)
    key = tmp_path / "key"
    key.write_bytes(Fernet.generate_key())
    key.chmod(0o600)
    return {
        "DOT_EVENTS_ENABLED": "1",
        "DOT_EXPECTED_USER_ID": "123",
        "DOT_EVENTS_PROFILES_FILE": str(profiles),
        "DOT_EVENTS_DB": str(private / "db"),
        "DOT_EVENTS_KEY_FILE": str(key),
    }


class Client:
    def __init__(self):
        self.handlers = []

    def add_event_handler(self, handler, builder):
        self.handlers.append(handler)

    def remove_event_handler(self, handler, builder):
        self.handlers.remove(handler)

    def is_connected(self):
        return False


def test_off_no_access():
    class NoAccess:
        def __getattr__(self, name):
            raise AssertionError(name)

    assert configure(NoAccess(), NoAccess(), env={}) is None


@pytest.mark.parametrize("bad_db", [False, True])
def test_actual_http_auth_context_cannot_send_and_manual_survives(tmp_path, bad_db, caplog):
    env = files(tmp_path)
    if bad_db:
        from pathlib import Path

        db = Path(env["DOT_EVENTS_DB"])
        db.write_bytes(b"not sqlite")
        db.chmod(0o600)
    mcp = FastMCP(stateless_http=True, json_response=True)

    @mcp.tool()
    def send_message(text: str) -> str:
        return "manual sent " + text

    client = Client()
    app = configure(mcp, client, env=env)

    async def run():
        async with app.lifespan(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8765"
            ) as http:
                assert (
                    await http.get("/.well-known/oauth-authorization-server")
                ).status_code == 404
                for method in ("GET", "POST"):
                    assert (await http.request(method, "/mcp")).status_code == 401
                headers = {
                    "Accept": "application/json, text/event-stream",
                    "Authorization": "Bearer " + "c" * 32,
                }
                rpc = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "send_message", "arguments": {"text": "unsafe"}},
                }
                response = await http.post("/mcp", headers=headers, json=rpc)
                assert response.status_code == 200
                assert response.json()["result"]["isError"]
                headers["Authorization"] = "Bearer " + "m" * 32
                response = await http.post("/mcp", headers=headers, json=rpc)
                assert "manual sent unsafe" in response.text

    asyncio.run(run())
    assert not client.handlers
    assert "m" * 32 not in caplog.text and "c" * 32 not in caplog.text


def test_profile_fail_closed(tmp_path):
    env = files(tmp_path)
    with pytest.raises(ValueError):
        load_profiles(env["DOT_EVENTS_PROFILES_FILE"], "456")
    os.chmod(env["DOT_EVENTS_PROFILES_FILE"], 0o644)
    with pytest.raises(ValueError):
        configure(FastMCP(), Client(), env=env)


def test_selected_repair_once_per_connection_and_shutdown(tmp_path):
    from dataclasses import replace
    from telegram_mcp.event_bridge.runtime import EventsRuntime
    from telegram_mcp.event_bridge.store import SQLiteStore
    from telegram_mcp.event_bridge.models import WatchScope, ScopeTarget
    from test_store import active
    from types import SimpleNamespace

    store = SQLiteStore(tmp_path / "db")
    _, watch = active(store)
    target = ScopeTarget("owner", -1001234567890)
    updated = replace(
        watch, version=watch.version + 1, active=replace(watch.active, scope=WatchScope((target,)))
    )
    store.compare_and_swap("owner", "watch", watch.version, updated)

    class ExistingClient(Client):
        connected = True

        def is_connected(self):
            return self.connected

    client = ExistingClient()
    runtime = EventsRuntime("owner", (), store, None, client)
    calls = []

    async def backfill(client_arg, watch_id, selected, *, limit):
        calls.append((client_arg, watch_id, selected, limit))
        raise TimeoutError()

    runtime.ingress = SimpleNamespace(backfill=backfill)

    async def run():
        await runtime._repair()
        await runtime._repair()
        assert len(calls) == 1 and calls[0] == (client, "watch", target, 100)
        client.connected = False
        await runtime._repair()
        client.connected = True
        await runtime._repair()
        assert len(calls) == 2

    asyncio.run(run())
    assert store.health("owner", "watch").gap_count >= 1
    store.close()


def test_context_sources_actual_http(tmp_path):
    from telegram_mcp.event_bridge.store import SQLiteStore
    from test_store import active, record

    env = files(tmp_path)
    with SQLiteStore(env["DOT_EVENTS_DB"]) as store:
        os.chmod(env["DOT_EVENTS_DB"], 0o600)
        _, watch = active(store, owner="123")
        source = store.append_source("123", "watch", watch.epoch, record())
    app = configure(FastMCP(stateless_http=True, json_response=True), Client(), env=env)

    async def run():
        async with app.lifespan(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8765"
            ) as http:
                response = await http.post(
                    "/mcp",
                    headers={
                        "Authorization": "Bearer " + "c" * 32,
                        "Accept": "application/json, text/event-stream",
                    },
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/call",
                        "params": {"name": "event_sources", "arguments": {"watch_id": "watch"}},
                    },
                )
                assert response.status_code == 200
                result = response.json()["result"]
                assert not result.get("isError")
                assert (
                    str(source.sequence) in response.text
                    and "telegram.message.created" in response.text
                )

    asyncio.run(run())


@pytest.mark.parametrize(
    "delivery,verified,count", [("0", "0", 0), ("1", "0", 0), ("0", "1", 0), ("1", "1", 3)]
)
def test_event_catalog_requires_both_live_gates(tmp_path, delivery, verified, count):
    from telegram_mcp.event_bridge.runtime import _open_runtime
    from telegram_mcp.event_bridge.protocol import EventPrincipal

    env = files(tmp_path)
    env.update(DOT_EVENTS_DELIVERY_ENABLED=delivery, DOT_EVENTS_CONTEXT_VERIFIED=verified)
    profiles = load_profiles(env["DOT_EVENTS_PROFILES_FILE"], "123")
    runtime = _open_runtime(env, "123", profiles, Client())
    result = asyncio.run(
        runtime.service.dispatch(EventPrincipal("123", profile_id="manual"), "events/list", {})
    )
    assert len(result["events"]) == count
    asyncio.run(runtime.close())
