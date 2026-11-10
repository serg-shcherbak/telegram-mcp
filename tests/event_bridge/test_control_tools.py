import json

import pytest
from mcp.server.fastmcp import FastMCP

from telegram_mcp.event_bridge.models import Capability, ScopeTarget
from telegram_mcp.event_bridge.store import SQLiteStore
from telegram_mcp.event_bridge.tools import WatchManager, register_watch_tools


@pytest.mark.asyncio
async def test_real_mcp_controls_preview_exact_scope_and_fence_lifecycle(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        manager = WatchManager(
            store,
            owner_id="owner",
            account_ids=("account",),
            id_factory=lambda: "watch",
            catalog=(Capability("telegram.message.created", "B", True, "fixture", ("batch",)),),
        )
        resolved = []

        async def resolve(target):
            resolved.append(target)
            return target

        mcp = FastMCP("controls")
        register_watch_tools(mcp, manager, resolve_target=resolve)

        async def call(name, **args):
            result = await mcp.call_tool(name, args)
            return json.loads(result[0][0].text)

        draft = await call(
            "watch_prepare",
            label="Customer",
            targets=[{"account_id": "account", "marked_chat_id": 42}],
            capabilities=["telegram.message.created"],
        )
        assert resolved == [ScopeTarget("account", 42)]
        assert draft["state"] == "draft"
        preview = await call("watch_preview", watch_id="watch", expected_revision=1)
        assert preview["scope"]["targets"] == [
            {"account_id": "account", "marked_chat_id": 42, "topic_id": None}
        ]
        assert not preview["policy"]["wake_dot"]
        active = await call(
            "watch_activate",
            watch_id="watch",
            expected_revision=1,
            expected_version=draft["version"],
        )
        paused = await call("watch_pause", watch_id="watch", expected_version=active["version"])
        assert paused["state"] == "paused" and paused["epoch"] > active["epoch"]
        resumed = await call("watch_resume", watch_id="watch", expected_version=paused["version"])
        revoked = await call("watch_revoke", watch_id="watch", expected_version=resumed["version"])
        await call("watch_delete", watch_id="watch", expected_version=revoked["version"])
        assert store.list("owner") == ()
        tools = {t.name: t for t in await mcp.list_tools()}
        assert tools["watch_preview"].annotations.readOnlyHint
        assert not tools["watch_activate"].annotations.readOnlyHint
        assert not tools["watch_delete"].annotations.readOnlyHint


@pytest.mark.asyncio
async def test_resolution_failure_does_not_save_an_unverified_draft(tmp_path):
    with SQLiteStore(tmp_path / "db") as store:
        manager = WatchManager(store, owner_id="owner", account_ids=("account",))

        async def resolve(target):
            raise ValueError("Selected chat is unavailable")

        mcp = FastMCP("controls")
        register_watch_tools(mcp, manager, resolve_target=resolve)
        with pytest.raises(Exception, match="Selected chat is unavailable"):
            await mcp.call_tool(
                "watch_prepare",
                {
                    "label": "Customer",
                    "targets": [{"account_id": "account", "marked_chat_id": 42}],
                    "capabilities": ["telegram.message.created"],
                },
            )
        assert store.list("owner") == ()
