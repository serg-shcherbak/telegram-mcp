import base64
import pytest
from cryptography.fernet import Fernet
from telegram_mcp.event_bridge.protocol import EventService, ProtocolError
from telegram_mcp.event_bridge.store import SQLiteStore
from test_store import active, NOW

SECRET = "whsec_" + base64.b64encode(b"a" * 32).decode()
URL = "https://receiver.example/events"


class Callbacks:
    def __init__(self):
        self.verified = []

    async def verify(self, *args):
        self.verified.append(args)


def params(watch):
    return dict(
        name="telegram.activity.batch_ready",
        arguments=dict(watch_id="watch", scope_version=watch.active.revision),
        delivery=dict(mode="webhook", url=URL, secret=SECRET),
    )


@pytest.mark.asyncio
async def test_lifecycle_restart_encryption_and_owner(tmp_path):
    path = tmp_path / "db"
    key = Fernet.generate_key()
    with SQLiteStore(path) as store:
        _, watch = active(store)
        p = params(watch)
        service = EventService(store, key=key, callbacks=Callbacks(), clock=lambda: NOW)
        assert len(service.list_events()["events"]) == 1
        result = await service.subscribe("owner", p)
        assert result["id"] == (await service.subscribe("owner", p))["id"]
        with pytest.raises(ProtocolError):
            await service.subscribe("other", p)
        assert URL.encode() not in path.read_bytes()
        assert SECRET.encode() not in path.read_bytes()
    with SQLiteStore(path) as store:
        service = EventService(store, key=key, callbacks=Callbacks(), clock=lambda: NOW)
        assert len(service.subscriptions("owner", "watch")) == 1
        await service.unsubscribe("owner", p)
        assert not service.subscriptions("owner", "watch")
        assert (
            store.get("owner", "watch").state.value == "active"
        )  # Explicit local-state permission survives.


@pytest.mark.asyncio
async def test_verification_epoch_fence(tmp_path):
    from dataclasses import replace
    from telegram_mcp.event_bridge.models import WatchState

    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        p = params(watch)

        class Revoke(Callbacks):
            async def verify(self, *a):
                store.compare_and_swap(
                    "owner",
                    "watch",
                    watch.version,
                    replace(
                        watch,
                        version=watch.version + 1,
                        epoch=watch.epoch + 1,
                        state=WatchState.REVOKED,
                    ),
                )

        service = EventService(
            store, key=Fernet.generate_key(), callbacks=Revoke(), clock=lambda: NOW
        )
        with pytest.raises(ProtocolError):
            await service.subscribe("owner", p)
        assert not service.subscriptions("owner", "watch")


@pytest.mark.asyncio
@pytest.mark.parametrize("json_response", [True, False])
async def test_real_sdk_endpoint_auth_and_tools_regression(tmp_path, json_response):
    import httpx
    from mcp.server.fastmcp import FastMCP
    from telegram_mcp.transport.http import create_streamable_http_app, install_events_adapter
    from telegram_mcp.event_bridge.protocol import EventPrincipal

    sdk = FastMCP("events-test", stateless_http=True, json_response=json_response)

    @sdk.tool()
    def ordinary(value: str) -> str:
        return value

    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=Callbacks(), clock=lambda: NOW
        )

        async def principal(scope):
            if dict(scope["headers"]).get(b"authorization") == b"Bearer owned":
                return EventPrincipal("owner", frozenset({"watch"}))
            if dict(scope["headers"]).get(b"authorization") == b"Bearer restricted":
                return EventPrincipal("owner", frozenset({"different"}))
            return None

        base_app = create_streamable_http_app(sdk)
        app = install_events_adapter(base_app, service, principal_resolver=principal)
        async with sdk.session_manager.run():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://localhost:8000"
            ) as client:
                assert (await client.get("/mcp")).status_code == 401
                headers = {
                    "Authorization": "Bearer owned",
                    "Accept": "application/json, text/event-stream",
                }

                async def rpc(method, params=None, auth=headers):
                    response = await client.post(
                        "/mcp",
                        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
                        headers=auth,
                    )
                    if response.headers.get("content-type", "").startswith("text/event-stream"):
                        import json

                        data = next(
                            line[5:].strip()
                            for line in response.text.splitlines()
                            if line.startswith("data:")
                        )
                        return httpx.Response(response.status_code, json=json.loads(data))
                    return response

                initialize = await rpc(
                    "initialize",
                    dict(
                        protocolVersion="2025-11-25",
                        capabilities={},
                        clientInfo=dict(name="test", version="1"),
                    ),
                )
                assert initialize.status_code == 200, initialize.text
                assert initialize.json()["result"]["capabilities"]["events"] == {}
                tools = await rpc("tools/list")
                assert tools.json()["result"]["tools"][0]["name"] == "ordinary"
                called = await rpc(
                    "tools/call", dict(name="ordinary", arguments=dict(value="works"))
                )
                assert called.json()["result"]["content"][0]["text"] == "works"
                events = await rpc("events/list")
                assert (
                    events.json()["result"]["events"][0]["name"] == "telegram.activity.batch_ready"
                )
                denied = await rpc(
                    "events/subscribe",
                    params(watch),
                    dict(headers, Authorization="Bearer restricted"),
                )
                assert denied.json()["error"]["code"] == -32001
                subscribed = await rpc("events/subscribe", params(watch))
                assert "id" in subscribed.json()["result"]
                assert (await rpc("events/unsubscribe", params(watch))).json()["result"] == {}
        # Disabled adapter uses the original SDK capabilities.
        assert service.callbacks.verified


@pytest.mark.asyncio
async def test_unsubscribe_and_expiry_pause_without_state_permission(tmp_path):
    from dataclasses import replace
    from datetime import timedelta

    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        changed = replace(
            watch,
            version=watch.version + 1,
            active=replace(watch.active, policy=replace(watch.active.policy, store_state=False)),
        )
        assert store.compare_and_swap("owner", "watch", watch.version, changed)
        now = [NOW]
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=Callbacks(), clock=lambda: now[0]
        )
        await service.subscribe("owner", dict(params(changed), ttlMs=1000))
        now[0] += timedelta(seconds=2)
        assert not service.subscriptions("owner", "watch")
        assert store.get("owner", "watch").state.value == "paused"


@pytest.mark.asyncio
async def test_rotation_foreign_cursor_and_callback_error(tmp_path):
    from datetime import timedelta
    from telegram_mcp.event_bridge.callbacks import CallbackError

    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        p = params(watch)
        now = [NOW]
        receiver = Callbacks()
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: now[0]
        )
        result = await service.subscribe("owner", p)
        secret2 = "whsec_" + base64.b64encode(b"b" * 32).decode()
        changed = dict(p, delivery=dict(p["delivery"], secret=secret2))
        refreshed = await service.subscribe("owner", changed)
        assert refreshed["id"] == result["id"]
        grant = service.subscriptions("owner")[0]
        assert grant["previous_secret"] == SECRET
        assert datetime_fromiso(grant["rotation_until"]) == NOW + timedelta(minutes=5)
        with pytest.raises(ProtocolError):
            await service.subscribe("owner", dict(changed, cursor=result["cursor"] + "bad"))

        class Failure(Callbacks):
            async def verify(self, *a):
                raise CallbackError("timeout")

        service.callbacks = Failure()
        with pytest.raises(ProtocolError) as error:
            await service.subscribe("owner", changed)
        assert error.value.code == -32015 and error.value.reason == "timeout"


def datetime_fromiso(value):
    from datetime import datetime

    return datetime.fromisoformat(value)


@pytest.mark.asyncio
async def test_unavailable_names_and_unknown_filters_denied_before_verification(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        receiver = Callbacks()
        service = EventService(
            store, key=Fernet.generate_key(), callbacks=receiver, clock=lambda: NOW
        )
        for name in (
            "telegram.message.created",
            "telegram.attention.due",
            "telegram.watch.health_changed",
        ):
            with pytest.raises(ProtocolError):
                await service.subscribe("owner", dict(params(watch), name=name))
        p = params(watch)
        p["arguments"]["owner_id"] = "owner"
        with pytest.raises(ProtocolError):
            await service.subscribe("owner", p)
        assert not receiver.verified


@pytest.mark.asyncio
async def test_trusted_profile_revocation_cancels_durable_grant(tmp_path):
    from telegram_mcp.event_bridge.protocol import EventPrincipal

    allowed = [True]
    with SQLiteStore(tmp_path / "db") as store:
        _, watch = active(store)
        service = EventService(
            store,
            key=Fernet.generate_key(),
            callbacks=Callbacks(),
            clock=lambda: NOW,
            grant_authorizer=lambda owner, profile, watch_id: allowed[0]
            and (owner, profile, watch_id) == ("owner", "context-a", "watch"),
        )
        principal = EventPrincipal("owner", frozenset({"watch"}), profile_id="context-a")
        await service.dispatch(principal, "events/subscribe", params(watch))
        assert service.subscriptions("owner")[0]["profile_id"] == "context-a"
        allowed[0] = False
        assert service.subscriptions("owner") == ()
        assert store.subscription_rows("owner") == ()


@pytest.mark.asyncio
async def test_disabled_adapter_does_not_advertise_events():
    import httpx
    from mcp.server.fastmcp import FastMCP
    from telegram_mcp.transport.http import create_streamable_http_app

    sdk = FastMCP("ordinary-only", stateless_http=True, json_response=True)
    app = create_streamable_http_app(sdk)
    async with sdk.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://localhost:8000"
        ) as client:
            result = await client.post(
                "/mcp",
                headers={"Accept": "application/json, text/event-stream"},
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": dict(
                        protocolVersion="2025-11-25",
                        capabilities={},
                        clientInfo=dict(name="test", version="1"),
                    ),
                },
            )
            assert result.status_code == 200
            assert "events" not in result.json()["result"]["capabilities"]
