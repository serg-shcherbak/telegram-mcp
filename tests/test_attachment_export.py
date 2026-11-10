import asyncio
import base64
import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from telethon import types
from dot_original_fixtures import fixtures

from telegram_mcp import access, runtime
from telegram_mcp.tools import chats, messages  # noqa: F401
from telegram_mcp.transport.http import create_streamable_http_app

TOKEN = "synthetic-originals-credential-0001"
PAYLOAD = b"synthetic-original\x00\xff\x80\r\n"


def message(payload=PAYLOAD, *, media_id=9007199254740993):
    document = SimpleNamespace(
        id=media_id,
        size=len(payload),
        mime_type="application/octet-stream",
        attributes=[types.DocumentAttributeFilename("original.bin")],
    )
    return SimpleNamespace(
        id=7,
        peer_id=types.PeerUser(1),
        media=SimpleNamespace(document=document),
        document=document,
        photo=None,
        file=SimpleNamespace(size=len(payload), name="original.bin"),
        grouped_id=9007199254740995,
        date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        edit_date=None,
        rich_message=None,
    )


class Client:
    def __init__(self, source=None, payload=PAYLOAD):
        self.source = source or message(payload)
        self.payload = payload
        self.download_calls = 0
        self.content_calls = 0
        self.before_download_return = None
        self.message_id = 7

    async def get_entity(self, identifier):
        return types.User(id=int(identifier), first_name="Synthetic")

    async def get_messages(self, entity, ids):
        self.content_calls += 1
        assert ids == self.message_id
        return self.source

    async def download_media(self, target, file, progress_callback):
        self.download_calls += 1
        path = Path(str(file) + ".bin")
        path.write_bytes(self.payload)
        progress_callback(len(self.payload), len(self.payload))
        if self.before_download_return:
            self.before_download_return()
        return str(path)


@pytest.fixture
def environment(tmp_path, monkeypatch):
    media_root = tmp_path / "media"
    media_root.mkdir(mode=0o700)
    config_path = tmp_path / "access.json"
    config = {
        "owner_id": 1,
        "media_root": str(media_root),
        "profiles": {"dot": {"token": TOKEN, "read": [1], "attachments": [1]}},
    }

    def write_config():
        config_path.write_text(json.dumps(config), encoding="utf-8")
        config_path.chmod(0o600)

    write_config()
    client = Client()
    monkeypatch.setattr(runtime, "clients", {"default": client})
    monkeypatch.setattr(runtime, "ALLOWED_CHAT_IDS", None)
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)

    async def connected(*args):
        pass

    monkeypatch.setattr(runtime, "ensure_connected", connected)
    return config_path, config, write_config, client, media_root


@asynccontextmanager
async def session_for(path):
    server = access.DotMCP(path, runtime.mcp)
    app = create_streamable_http_app(server)
    async with server.session_manager.run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app),
            base_url="http://127.0.0.1:8765",
            headers={"x-dot-backend-token": TOKEN},
        ) as client:
            async with streamable_http_client(
                "http://127.0.0.1:8765/mcp", http_client=client
            ) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    yield session


def text_of(result):
    return "\n".join(getattr(item, "text", "") for item in result.content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "original_fixture", [None, *fixtures()], ids=lambda item: item["name"] if item else "binary"
)
async def test_original_bytes_survive_real_sdk_http(environment, original_fixture):
    path, _, _, client, root = environment
    payload = PAYLOAD
    album_id = "9007199254740995"
    if original_fixture:
        payload = original_fixture["data"]
        client.payload = payload
        client.source = message(payload)
        client.message_id = client.source.id = original_fixture["message_id"]
        client.source.file.name = original_fixture["name"]
        client.source.document.mime_type = original_fixture["mime"]
        client.source.grouped_id = original_fixture["grouped_id"]
        album_id = str(original_fixture["grouped_id"]) if original_fixture["grouped_id"] else None
    async with session_for(path) as session:
        names = {tool.name for tool in (await session.list_tools()).tools}
        assert "export_attachment" in names
        result = await session.call_tool(
            "export_attachment", {"chat_id": 1, "message_id": client.message_id}
        )
    assert not result.isError, text_of(result)
    metadata = json.loads(result.content[0].text)
    blob = result.content[1].resource
    assert base64.b64decode(blob.blob, validate=True) == payload
    assert metadata["sha256"] == hashlib.sha256(payload).hexdigest()
    assert metadata["source"]["media_id"] == "9007199254740993"
    assert metadata["source"]["album_id"] == album_id
    assert len(result.model_dump_json().encode()) < 10 * 1024 * 1024
    assert str(blob.uri).startswith("telegram://")
    assert str(root) not in result.model_dump_json()
    assert client.content_calls == 2
    assert client.download_calls == 1
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read,attachments",
    [([], [1]), ([1], []), ([1], [2]), ("all", []), ([], "all")],
)
async def test_export_requires_both_source_scopes(environment, read, attachments):
    path, config, write_config, client, root = environment
    config["profiles"]["dot"].update(read=read, attachments=attachments)
    write_config()
    async with session_for(path) as session:
        names = {tool.name for tool in (await session.list_tools()).tools}
        assert "export_attachment" not in names
        denied = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert denied.isError
    assert client.content_calls == client.download_calls == 0
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        {"chat_id": True, "message_id": 7},
        {"chat_id": 1.0, "message_id": 7},
        {"chat_id": 1, "message_id": True},
        {"chat_id": 1, "message_id": 7.0},
        {"chat_id": 1, "message_id": "7"},
        {"chat_id": 1, "message_id": 0},
        {"chat_id": 1, "message_id": 2**31},
        {"chat_id": 1, "message_id": 7, "media_id": True},
        {"chat_id": 1, "message_id": 7, "media_id": 9007199254740993},
        {"chat_id": 1, "message_id": 7, "media_id": "9223372036854775808"},
        {"chat_id": 1, "message_id": 7, "media_id": "1e10"},
        {"chat_id": 1, "message_id": 7, "account": "other"},
        {"chat_id": 1, "message_id": 7, "file_path": "/private/session"},
        {"chat_id": 1, "message_id": 7, "profile": "other"},
    ],
)
async def test_export_rejects_spoofed_and_malformed_arguments(environment, arguments):
    path, _, _, client, root = environment
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", arguments)
    assert result.isError
    assert client.download_calls == 0
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_export_rejects_actual_peer_namespace_mismatch(environment):
    path, _, _, client, root = environment

    async def wrong_peer(identifier):
        return types.Chat(
            id=1,
            title="Restricted",
            photo=types.ChatPhotoEmpty(),
            participants_count=0,
            date=None,
            version=0,
        )

    client.get_entity = wrong_peer
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert client.content_calls == client.download_calls == 0
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_export_rejects_message_from_wrong_source(environment):
    path, _, _, client, root = environment
    client.source.peer_id = types.PeerUser(2)
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert client.download_calls == 0
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["before", "during", "after"])
async def test_revoked_export_returns_no_original(environment, when):
    path, config, write_config, client, root = environment

    def revoke():
        config["profiles"]["dot"]["attachments"] = []
        write_config()

    if when == "before":
        revoke()
    elif when == "during":
        client.before_download_return = revoke
    async with session_for(path) as session:
        first = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
        if when == "after":
            assert not first.isError
            revoke()
            first = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert first.isError
    assert all(item.type != "resource" for item in first.content)
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["id", "edit", "deleted"])
async def test_changed_attachment_is_withheld(environment, change):
    path, _, _, client, root = environment

    def changed():
        if change == "id":
            client.source.document.id += 1
        elif change == "edit":
            client.source.edit_date = datetime.now(timezone.utc)
        else:
            client.source = None

    client.before_download_return = changed
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert all(item.type != "resource" for item in result.content)
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["declared", "progress", "actual", "short", "missing", "error"]
)
async def test_bounded_export_failures_clean_all_staging(environment, monkeypatch, failure):
    from telegram_mcp import attachments

    path, _, _, client, root = environment
    monkeypatch.setattr(attachments, "MAX_ORIGINAL_BYTES", 4)
    client.payload = b"12345" if failure in ("declared", "progress", "actual") else b"123"
    client.source = message(client.payload)
    if failure != "declared":
        client.source.file.size = None if failure != "short" else 4
        client.source.document.size = client.source.file.size
    if failure in ("actual", "missing", "error"):

        async def broken_download(target, file, progress_callback):
            client.download_calls += 1
            result = Path(str(file) + ".part")
            result.write_bytes(client.payload)
            if failure == "error":
                raise RuntimeError("synthetic network failure with private detail")
            return None if failure == "missing" else str(result)

        client.download_media = broken_download
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert client.download_calls == (0 if failure == "declared" else 1)
    assert "private detail" not in text_of(result)
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_cancelled_export_cleans_partial_original(environment):
    from telegram_mcp.attachments import export_attachment

    path, _, _, client, root = environment
    started = asyncio.Event()

    async def interrupted(target, file, progress_callback):
        Path(str(file) + ".part").write_bytes(b"partial")
        started.set()
        await asyncio.Event().wait()

    client.download_media = interrupted
    config = access.load_access_config(path)
    connection_token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    operation_token = access.current_operation.set("attachments")
    try:
        task = asyncio.create_task(export_attachment(1, 7))
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        access.current_operation.reset(operation_token)
        access.current_connection.reset(connection_token)
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_export_sanitizes_metadata_and_exposes_no_credentials(environment):
    path, _, _, client, _ = environment
    client.source.file.name = "../../private\\original\x00.bin"
    client.source.document.mime_type = "text/plain\r\nsecret"
    client.source.document.access_hash = 987654321
    client.source.document.file_reference = b"private-reference"
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert not result.isError
    metadata = json.loads(result.content[0].text)
    assert metadata["filename"] == "original.bin"
    assert metadata["mime_type"] == "application/octet-stream"
    serialized = result.model_dump_json()
    assert TOKEN not in serialized
    assert "access_hash" not in serialized
    assert "file_reference" not in serialized
    assert "private-reference" not in serialized


@pytest.mark.asyncio
async def test_legacy_ceiling_still_restricts_export(environment, monkeypatch):
    path, config, write_config, client, _ = environment
    config["profiles"]["dot"].update(read="all", attachments="all")
    write_config()
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "2")
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert client.content_calls == client.download_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("selected", ["9007199254740993", "9", None])
async def test_rich_media_is_selected_only_from_current_message(environment, selected):
    path, _, _, client, root = environment
    document = client.source.document
    client.source.media = client.source.document = client.source.file = None
    client.source.rich_message = SimpleNamespace(
        blocks=[types.PageBlockVideo(video_id=document.id, caption=None)],
        documents=[document],
        photos=[],
    )
    async with session_for(path) as session:
        arguments = {"chat_id": 1, "message_id": 7}
        if selected is not None:
            arguments["media_id"] = selected
        result = await session.call_tool("export_attachment", arguments)
    assert result.isError == (selected == "9")
    assert client.download_calls == (0 if selected == "9" else 1)
    if not result.isError:
        metadata = json.loads(result.content[0].text)
        assert metadata["source"]["media_id"] == str(document.id)
        assert metadata["filename"] == "original.bin"
        assert base64.b64decode(result.content[1].resource.blob) == PAYLOAD
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_regular_media_rejects_rich_media_selector(environment):
    path, _, _, client, root = environment
    async with session_for(path) as session:
        result = await session.call_tool(
            "export_attachment", {"chat_id": 1, "message_id": 7, "media_id": "9007199254740993"}
        )
    assert result.isError
    assert "regular media" in text_of(result)
    assert client.download_calls == 0
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_export_never_reads_a_downloader_path_outside_its_staging(environment, tmp_path):
    path, _, _, client, root = environment
    sentinel = tmp_path / "not-an-attachment"
    sentinel.write_bytes(b"private-sentinel")

    async def outside(target, file, progress_callback):
        return str(sentinel)

    client.download_media = outside
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert "private-sentinel" not in text_of(result)
    assert sentinel.read_bytes() == b"private-sentinel"
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_serialized_result_limit_is_checked_before_return(environment, monkeypatch):
    from telegram_mcp import attachments

    path, _, _, _, root = environment
    monkeypatch.setattr(attachments, "MAX_SERIALIZED_BYTES", 4097)
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert result.isError
    assert "transport limit" in text_of(result)
    assert all(item.type != "resource" for item in result.content)
    assert list(root.iterdir()) == []


@pytest.mark.asyncio
async def test_http_client_cancellation_cleans_partial_original(environment):
    path, _, _, client, root = environment
    started = asyncio.Event()

    async def interrupted(target, file, progress_callback):
        Path(str(file) + ".part").write_bytes(b"partial")
        started.set()
        await asyncio.Event().wait()

    client.download_media = interrupted
    async with session_for(path) as session:
        task = asyncio.create_task(
            session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert list(root.iterdir()) == []


def test_export_is_registered_only_on_safe_profile():
    assert runtime.mcp._tool_manager.get_tool("export_attachment") is None


@pytest.mark.asyncio
async def test_full_provisional_raw_limit_fits_real_sdk_response(environment):
    from telegram_mcp.attachments import MAX_ORIGINAL_BYTES, MAX_SERIALIZED_BYTES

    path, _, _, client, root = environment
    client.payload = b"\x00" * MAX_ORIGINAL_BYTES
    client.source = message(client.payload)
    async with session_for(path) as session:
        result = await session.call_tool("export_attachment", {"chat_id": 1, "message_id": 7})
    assert not result.isError, text_of(result)
    assert len(result.model_dump_json().encode()) < MAX_SERIALIZED_BYTES
    received = base64.b64decode(result.content[1].resource.blob, validate=True)
    assert len(received) == MAX_ORIGINAL_BYTES
    assert hashlib.sha256(received).hexdigest() == hashlib.sha256(client.payload).hexdigest()
    assert list(root.iterdir()) == []
