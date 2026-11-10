"""Check an installed Dot profile with synthetic state, without connecting to Telegram.

Run with the installed Python: python -I smoke_dot_install.py /path/to/dot_server.py
The same helper can be supplied on stdin inside a network-disabled image.
"""

import asyncio
from importlib import metadata, util
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Supply the absolute Dot entrypoint path")
    entrypoint = Path(sys.argv[1]).resolve(strict=True)
    import telegram_mcp

    assert Path(sys.prefix).resolve() in Path(telegram_mcp.__file__).resolve().parents
    origin = json.loads(metadata.distribution("telegram-mcp").read_text("direct_url.json"))
    assert not origin.get("dir_info", {}).get("editable", False)
    for key in list(os.environ):
        if key.startswith(("TELEGRAM_", "DOT_", "MCP_")):
            del os.environ[key]
    os.environ["PYTHON_DOTENV_DISABLED"] = "1"
    spec = util.spec_from_file_location("installed_dot_server", entrypoint)
    module = util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with tempfile.TemporaryDirectory(prefix="dot-install-synthetic-") as directory:
        root = Path(directory)
        root.chmod(0o700)
        config_path = root / "access.json"
        session = root / "owner.session"
        session.touch(mode=0o600)
        token = secrets.token_hex(32)
        config = {
            "owner_id": 1,
            "media_root": str(root / "media"),
            "profiles": {"dot": {"token": "REPLACE_ME"}},
        }

        def write_config():
            config_path.write_text(json.dumps(config), encoding="utf-8")
            config_path.chmod(0o600)

        os.environ.update(
            DOT_ACCESS_CONFIG=str(config_path),
            TELEGRAM_API_ID="12345",
            TELEGRAM_API_HASH="synthetic-offline-api-hash",
            TELEGRAM_SESSION_NAME=str(session),
            TELEGRAM_TRANSCRIBE="off",
            TELEGRAM_WARM_ENTITY_CACHE="0",
            TELEGRAM_TRANSCRIPT_CACHE_DIR=str(root / "transcripts"),
            TELEGRAM_AUDIO_SCRATCH_DIR=str(root / "audio"),
        )
        write_config()
        from telegram_mcp.access import AccessDenied

        try:
            module.prepare()
        except AccessDenied:
            pass
        else:
            raise AssertionError("Placeholder backend credential was accepted")
        config["profiles"]["dot"]["token"] = token
        write_config()
        os.environ["TELEGRAM_EVENT_FEED"] = "1"
        try:
            module.prepare()
        except ValueError as exc:
            assert "TELEGRAM_EVENT_FEED" in str(exc)
        else:
            raise AssertionError("Observation enabled in the direct-task profile")
        os.environ["TELEGRAM_EVENT_FEED"] = "0"
        runner = module.prepare()
        asyncio.run(check_transport(runner.mcp, token, config, write_config))
    print(
        "Installed profile: non-editable imports, safe defaults, authenticated MCP, revocation OK"
    )
    print("Telegram connection, external ASR and host/tunnel acceptance: not exercised")


async def check_transport(server, token, config, write_config):
    import httpx
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client
    from telegram_mcp.transport.http import create_streamable_http_app

    app = create_streamable_http_app(server)
    async with server.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://127.0.0.1:8765"
        ) as http:
            for headers in ({}, {"x-dot-backend-token": "invalid"}):
                assert (await http.get("/mcp", headers=headers)).status_code == 401
            http.headers["x-dot-backend-token"] = token
            async with streamable_http_client(
                "http://127.0.0.1:8765/mcp", http_client=http
            ) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    tools = {tool.name for tool in (await session.list_tools()).tools}
                    privileged = {"export_attachment", "transcribe_voice", "send_message"}
                    assert "get_messages" in tools
                    assert not privileged & tools
                    assert not {"delete_message", "mark_as_read", "subscribe"} & tools
                    for operation in ("attachments", "external_asr", "send"):
                        config["profiles"]["dot"][operation] = [1]
                    write_config()
                    tools = {tool.name for tool in (await session.list_tools()).tools}
                    assert {"get_messages"} | privileged <= tools
                    for operation in ("attachments", "external_asr", "send"):
                        config["profiles"]["dot"][operation] = []
                    write_config()
                    tools = {tool.name for tool in (await session.list_tools()).tools}
                    assert not privileged & tools
            config["profiles"]["dot"]["token"] = secrets.token_hex(32)
            write_config()
            assert (await http.get("/mcp")).status_code == 401
    from telegram_mcp import runtime

    assert all(not client.is_connected() for client in runtime.clients.values())
    for client in runtime.clients.values():
        client.session.close()


if __name__ == "__main__":
    main()
