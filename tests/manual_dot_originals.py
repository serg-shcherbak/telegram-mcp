"""Loopback-only synthetic Dot staging: use get_messages and export_attachment.

Run with the repository's Python environment. No Telegram login, real session,
provider key or tunnel is used. Other catalog reads may be unsupported by the fake.
"""

import argparse
import asyncio
import base64
import hashlib
import inspect
import json
import os
import secrets
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

from dot_original_fixtures import fixtures


def build_server(root, port):
    if "telegram_mcp.runtime" in sys.modules:
        raise RuntimeError("Run this synthetic fixture server as a separate process")
    os.environ.clear()
    os.environ.update(
        PYTHON_DOTENV_DISABLED="1",
        TELEGRAM_API_ID="12345",
        TELEGRAM_API_HASH="synthetic_only",
        TELEGRAM_SESSION_NAME=":memory:",
        TELEGRAM_TRANSCRIBE="off",
        TELEGRAM_LEGACY_EVENTS="0",
        TELEGRAM_EVENT_FEED="0",
        TELEGRAM_SERVER_ROOTS_ONLY="1",
        TELEGRAM_ALIASES_FILE=str(root / "aliases.json"),
        XDG_STATE_HOME=str(root / "state"),
    )
    os.chdir(root)
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from telethon import types

    from telegram_mcp import access, runtime
    from telegram_mcp.tools import media, messages
    from telegram_mcp import attachments

    records = fixtures()
    by_id = {record["message_id"]: record for record in records}
    date = datetime(2020, 1, 1, tzinfo=timezone.utc)
    sources = {}
    for record in records:
        document = types.Document(
            id=9000 + record["message_id"],
            access_hash=0,
            file_reference=b"synthetic",
            date=date,
            mime_type=record["mime"],
            size=len(record["data"]),
            dc_id=0,
            attributes=[types.DocumentAttributeFilename(record["name"])],
        )
        sources[record["message_id"]] = types.Message(
            id=record["message_id"],
            peer_id=types.PeerUser(1),
            from_id=types.PeerUser(1),
            date=date,
            message="Synthetic original: " + record["name"],
            media=types.MessageMediaDocument(document=document),
            grouped_id=record["grouped_id"],
        )

    class SyntheticClient:
        async def get_entity(self, identifier):
            if identifier != 1:
                raise ValueError("Synthetic source 1 is the only available chat")
            return types.User(id=1, first_name="Synthetic source")

        async def get_messages(self, entity, ids=None, limit=20, add_offset=0, **kwargs):
            if entity.id != 1:
                raise ValueError("Synthetic source 1 is the only available chat")
            if ids is not None:
                return sources.get(ids)
            return list(sources.values())[add_offset : add_offset + limit]

        async def download_media(self, target, file, progress_callback=None):
            data = by_id[target.id]["data"]
            Path(file).write_bytes(data)
            if progress_callback:
                progress = progress_callback(len(data), len(data))
                if inspect.isawaitable(progress):
                    await progress
            return str(file)

    client = SyntheticClient()

    async def ensure_synthetic(cl=None):
        if cl is not None and cl is not client:
            raise RuntimeError("Real Telegram clients are disabled in this fixture server")

    runtime.clients = {"default": client}
    for module in (runtime, media, messages, attachments):
        module.ensure_connected = ensure_synthetic
    media_root = root / "media"
    media_root.mkdir(mode=0o700)
    runtime.SERVER_ALLOWED_ROOTS = [media_root]
    token = secrets.token_urlsafe(32)
    config_path = root / "access.json"
    config = {
        "owner_id": 1,
        "media_root": str(media_root),
        "profiles": {"synthetic": {"token": token, "read": [1], "attachments": [1], "send": []}},
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    (root / "backend-token").write_text(token, encoding="ascii")
    manifest = [
        {key: value for key, value in record.items() if key != "data"}
        | {
            "chat_id": "1",
            "size": len(record["data"]),
            "sha256": hashlib.sha256(record["data"]).hexdigest(),
        }
        for record in records
    ]
    (root / "answers.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    for name in ("access.json", "backend-token", "answers.json"):
        (root / name).chmod(0o600)
    server = access.DotMCP(config_path, runtime.mcp)
    server.settings.host = "127.0.0.1"
    server.settings.port = port
    return server, token, records


async def self_check(server, token, records):
    import httpx
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    from telegram_mcp.transport.http import create_streamable_http_app

    app = create_streamable_http_app(server)
    async with server.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app),
            headers={"x-dot-backend-token": token},
        ) as http:
            async with streamable_http_client(
                f"http://127.0.0.1:{server.settings.port}/mcp", http_client=http
            ) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    listed = await session.call_tool("get_messages", {"chat_id": 1})
                    assert not listed.isError, listed
                    assert "album:901" in listed.content[0].text
                    for record in records:
                        started = time.perf_counter()
                        result = await session.call_tool(
                            "export_attachment",
                            {"chat_id": 1, "message_id": record["message_id"]},
                        )
                        assert not result.isError, result
                        resource = next(
                            item.resource for item in result.content if item.type == "resource"
                        )
                        received = base64.b64decode(resource.blob, validate=True)
                        assert received == record["data"]
                        print(
                            json.dumps(
                                {
                                    "name": record["name"],
                                    "bytes": len(received),
                                    "serialized_result_bytes": len(
                                        result.model_dump_json(exclude_none=True).encode("utf-8")
                                    ),
                                    "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                                    "sha256": hashlib.sha256(received).hexdigest(),
                                }
                            )
                        )
    assert not list(server._dot_media_root.iterdir())
    print("Local SDK originals verified. Dot materialization: NOT TESTED.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if not 0 < args.port < 65536:
        parser.error("port must be between 1 and 65535")
    os.umask(0o077)
    with tempfile.TemporaryDirectory(prefix="dot-originals-synthetic-") as directory:
        root = Path(directory).resolve()
        server, token, records = build_server(root, args.port)
        print(f"Synthetic MCP endpoint: http://127.0.0.1:{args.port}/mcp", flush=True)
        print(f"Backend header x-dot-backend-token: file {root / 'backend-token'}", flush=True)
        print(f"Owner-only known answers: {root / 'answers.json'}", flush=True)
        if args.self_check:
            asyncio.run(self_check(server, token, records))
        else:
            asyncio.run(server.run_streamable_http_async())


if __name__ == "__main__":
    main()
