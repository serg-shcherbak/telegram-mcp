import httpx
import pytest
from cryptography.fernet import Fernet
from mcp.server.fastmcp import FastMCP

from telegram_mcp.event_bridge.protocol import EventPrincipal, EventService
from telegram_mcp.event_bridge.store import SQLiteStore
from telegram_mcp.transport.http import create_streamable_http_app, install_events_adapter


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"Host": "attacker.invalid"}, 421),
        ({"Origin": "https://attacker.invalid"}, 403),
        ({"Content-Type": "text/plain"}, 400),
        ({"MCP-Protocol-Version": "2100-01-01"}, 400),
    ],
)
async def test_custom_rpc_keeps_sdk_transport_validation(tmp_path, overrides, expected):
    sdk = FastMCP("secured-events", stateless_http=True, json_response=True)
    with SQLiteStore(tmp_path / "db") as store:
        service = EventService(store, key=Fernet.generate_key())

        async def principal(scope):
            return EventPrincipal("owner")

        app = install_events_adapter(
            create_streamable_http_app(sdk),
            service,
            principal_resolver=principal,
            transport_security=sdk.settings.transport_security,
        )
        async with sdk.session_manager.run():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://localhost:8000"
            ) as client:
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/event-stream",
                    **overrides,
                }
                result = await client.post(
                    "/mcp",
                    headers=headers,
                    content=b'{"jsonrpc":"2.0","id":1,"method":"events/list"}',
                )
                assert result.status_code == expected
