"""Synthetic connection-policy and local MCP transport regressions."""

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server.fastmcp import FastMCP
from telethon import types

from telegram_mcp import access, runtime
from telegram_mcp import transcription
from telegram_mcp.tools import chats, messages
from telegram_mcp.transport.http import create_streamable_http_app


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/mcp"),
        ("OPTIONS", "/mcp"),
        ("POST", "/mcp"),
        ("DELETE", "/mcp"),
        ("GET", "/original/1"),
    ],
)
async def test_backend_auth_precedes_transport_responses(tmp_path, method, path):
    server = FastMCP("safe", stateless_http=True)
    server._dot_access_config = tmp_path / "missing.json"
    app = create_streamable_http_app(server)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://127.0.0.1:8765"
    ) as client:
        response = await client.request(method, path)
    assert response.status_code == 401


TOKEN_A = "synthetic-profile-a-credential-0001"
TOKEN_B = "synthetic-profile-b-credential-0002"


def _write_config(path, data):
    path.write_text(json.dumps(data), encoding="utf-8")
    path.chmod(0o600)


@pytest.fixture
def owner_config(tmp_path):
    root = tmp_path / "media"
    root.mkdir(mode=0o700)
    path = tmp_path / "access.json"
    data = {
        "owner_id": 1,
        "media_root": str(root),
        "profiles": {
            "reader": {"token": TOKEN_A, "read": [1]},
            "sender": {"token": TOKEN_B, "read": [2], "send": [2]},
        },
    }
    _write_config(path, data)
    return path, data


class _Client:
    def __init__(self):
        self.users = {
            1: types.User(id=1, first_name="Visible one"),
            2: types.User(id=2, first_name="Hidden two"),
        }
        self.sent = []
        self.dialog_calls = 0
        self.content_calls = 0
        self.before_return = None
        self.resolve_as = None
        self.handlers = []

    def list_event_handlers(self):
        return list(self.handlers)

    def remove_event_handler(self, callback):
        self.handlers = [item for item in self.handlers if item[0] is not callback]

    async def get_dialogs(self, **kwargs):
        self.dialog_calls += 1
        await asyncio.sleep(0)
        if self.before_return:
            self.before_return()
        return [SimpleNamespace(entity=user) for user in self.users.values()]

    async def get_entity(self, identifier):
        return self.resolve_as or self.users[int(identifier)]

    async def get_messages(self, entity, **kwargs):
        self.content_calls += 1
        return []

    async def send_message(self, entity, text, **kwargs):
        self.sent.append((entity.id, text, kwargs.get("reply_to")))
        return SimpleNamespace(id=55)


@pytest.fixture
def core_client(monkeypatch):
    client = _Client()
    monkeypatch.setattr(runtime, "clients", {"default": client})
    monkeypatch.setattr(runtime, "ALLOWED_CHAT_IDS", None)
    monkeypatch.delenv("TELEGRAM_ALLOWED_CHAT_IDS", raising=False)
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE", "on-demand")

    async def connected(*args):
        pass

    for module in (runtime, chats, messages):
        monkeypatch.setattr(module, "ensure_connected", connected)
    return client


@asynccontextmanager
async def _server(path):
    server = access.DotMCP(path, runtime.mcp)
    app = create_streamable_http_app(server)
    async with server.session_manager.run():
        yield server, app


@asynccontextmanager
async def _session(app, token):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app),
        base_url="http://127.0.0.1:8765",
        headers={"x-dot-backend-token": token},
    ) as client:
        async with streamable_http_client(
            "http://127.0.0.1:8765/mcp", http_client=client
        ) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                yield session


def _text(result):
    return "\n".join(getattr(item, "text", "") for item in result.content)


@pytest.fixture
def direct_audio(owner_config, core_client, monkeypatch, transcript_cache_dir):
    from datetime import datetime, timezone

    path, data = owner_config
    data["profiles"]["reader"]["external_asr"] = [1]
    _write_config(path, data)
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "https://api.openai.com/v1")
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_OPENAI_API_KEY", "synthetic-key")
    monkeypatch.setenv("TELEGRAM_TRANSCRIBE_OPENAI_MODEL", "synthetic-model")
    msg = types.Message(
        id=7,
        peer_id=types.PeerUser(1),
        date=datetime.now(timezone.utc),
        message="",
        media=types.MessageMediaDocument(
            document=types.Document(
                id=101,
                access_hash=0,
                file_reference=b"synthetic",
                date=None,
                mime_type="audio/mpeg",
                size=12,
                dc_id=1,
                attributes=[types.DocumentAttributeAudio(duration=5, voice=False)],
            )
        ),
    )
    state = {
        "message": msg,
        "engines": [],
        "fetches": 0,
        "real_transcribe": transcription.transcribe,
    }

    async def fetch(entity, **kwargs):
        state["fetches"] += 1
        return state["message"] if "ids" in kwargs else [state["message"]]

    async def provider(cl, entity, source, engine):
        state["engines"].append(engine)
        return {"status": "ok", "text": "synthetic transcript"}

    monkeypatch.setattr(core_client, "get_messages", fetch)
    monkeypatch.setattr(transcription, "transcribe", provider)
    return state


@pytest.mark.asyncio
async def test_direct_audio_uses_openai_without_attachment_export_grant(
    owner_config, core_client, direct_audio
):
    path, _ = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            names = {tool.name for tool in (await session.list_tools()).tools}
            assert {"transcribe_voice", "get_message_context", "list_messages"} <= names
            assert "export_attachment" not in names
            first = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert not first.isError, _text(first)
            payload = json.loads(_text(first))
            assert payload["source"] == "openai"
            assert payload["complete"] is True
            assert payload["fallback_reason"] is None
            second = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert json.loads(_text(second))["cached"] is True
            assert direct_audio["fetches"] >= 2
    assert direct_audio["engines"] == ["openai"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["no_opt_in", "no_read", "wrong_profile", "wrong_source", "removed_source"]
)
async def test_direct_audio_warm_cache_cannot_bypass_current_source_and_scope(
    owner_config, core_client, direct_audio, change
):
    path, data = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            first = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert not first.isError, _text(first)
            if change == "no_opt_in":
                data["profiles"]["reader"]["external_asr"] = []
            elif change == "no_read":
                data["profiles"]["reader"]["read"] = []
            elif change == "wrong_source":
                direct_audio["message"].peer_id = types.PeerUser(2)
            elif change == "wrong_profile":
                pass
            else:
                direct_audio["message"] = None
            _write_config(path, data)
            if change == "wrong_profile":
                async with _session(app, TOKEN_B) as other:
                    denied = await other.call_tool(
                        "transcribe_voice", {"chat_id": 1, "message_id": 7}
                    )
            else:
                denied = await session.call_tool(
                    "transcribe_voice", {"chat_id": 1, "message_id": 7}
                )
            assert denied.isError, _text(denied)
            assert "synthetic transcript" not in _text(denied)
    assert direct_audio["engines"] == ["openai"]


@pytest.mark.asyncio
@pytest.mark.parametrize("engine", ["groq", "whisper", "telegram"])
async def test_private_audio_cannot_choose_another_provider(
    owner_config, core_client, direct_audio, engine
):
    path, _ = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool(
                "transcribe_voice", {"chat_id": 1, "message_id": 7, "engine": engine}
            )
            assert result.isError
    assert direct_audio["engines"] == []


@pytest.mark.asyncio
async def test_history_never_starts_asr_or_returns_unpermitted_transcripts(
    owner_config, core_client, direct_audio, monkeypatch
):
    path, data = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            data["profiles"]["reader"]["external_asr"] = []
            _write_config(path, data)
            monkeypatch.setenv("TELEGRAM_TRANSCRIBE", "auto")
            result = await session.call_tool("list_messages", {"chat_id": 1})
            assert not result.isError, _text(result)
            assert "synthetic transcript" not in _text(result)
    assert direct_audio["engines"] == ["openai"]


@pytest.mark.asyncio
async def test_asr_revocation_during_provider_withholds_and_does_not_cache(
    owner_config, core_client, direct_audio, monkeypatch
):
    path, data = owner_config

    async def revoked(*args):
        data["profiles"]["reader"]["external_asr"] = []
        _write_config(path, data)
        return {"status": "ok", "text": "withheld transcript"}

    monkeypatch.setattr(transcription, "transcribe", revoked)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert result.isError
            assert "withheld transcript" not in _text(result)
    assert transcription.get_cached_transcript(1, 7) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["model", "language", "attachment", "account", "session_account", "expiry"]
)
async def test_direct_audio_cache_identity_and_retention(
    owner_config, core_client, direct_audio, monkeypatch, change
):
    import sqlite3

    path, data = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            first = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert not first.isError, _text(first)
            if change == "model":
                monkeypatch.setenv("TELEGRAM_TRANSCRIBE_OPENAI_MODEL", "changed-model")
            elif change == "language":
                monkeypatch.setenv("TELEGRAM_TRANSCRIBE_LANGUAGE", "ru")
            elif change == "attachment":
                direct_audio["message"].media.document.id += 1
            elif change == "account":
                data["owner_id"] = 3
                _write_config(path, data)
            elif change == "session_account":
                monkeypatch.setattr(
                    core_client, "_mb_entity_cache", SimpleNamespace(self_id=2), raising=False
                )
            else:
                with sqlite3.connect(transcription._cache_db_path()) as conn:
                    conn.execute("UPDATE transcripts SET expires_at = 0")
            second = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            if change == "account":
                assert second.isError
            else:
                assert not second.isError, _text(second)
                assert json.loads(_text(second))["cached"] is False
    assert len(direct_audio["engines"]) == (1 if change == "account" else 2)


@pytest.mark.asyncio
async def test_context_keeps_whole_mixed_album_and_private_forward_origin(
    owner_config, core_client, direct_audio, monkeypatch
):
    from datetime import datetime, timezone

    path, _ = owner_config
    now = datetime.now(timezone.utc)
    album = [
        types.Message(
            id=6,
            peer_id=types.PeerUser(1),
            date=now,
            message="Whole post caption",
            grouped_id=901,
            media=types.MessageMediaPhoto(
                photo=types.Photo(
                    id=501, access_hash=0, file_reference=b"x", date=now, sizes=[], dc_id=1
                )
            ),
            fwd_from=types.MessageFwdHeader(date=now, from_name="Private author"),
        ),
        types.Message(
            id=7,
            peer_id=types.PeerUser(1),
            date=now,
            message="Video caption",
            grouped_id=901,
            media=types.MessageMediaDocument(
                document=types.Document(
                    id=502,
                    access_hash=0,
                    file_reference=b"x",
                    date=now,
                    mime_type="video/mp4",
                    size=12,
                    dc_id=1,
                    attributes=[types.DocumentAttributeVideo(duration=4, w=32, h=32)],
                )
            ),
        ),
    ]

    async def fetch(entity, **kwargs):
        if "ids" in kwargs:
            return next((item for item in album if item.id == kwargs["ids"]), None)
        if "max_id" in kwargs:
            return [item for item in reversed(album) if item.id < kwargs["max_id"]]
        return [item for item in album if item.id > kwargs["min_id"]]

    monkeypatch.setattr(core_client, "get_messages", fetch)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool(
                "get_message_context", {"chat_id": 1, "message_id": 7, "context_size": 0}
            )
            assert not result.isError, _text(result)
            payload = json.loads(_text(result)[_text(result).index("{") :])
            assert [(row["id"], row["media_id"], row["media"]) for row in payload["results"]] == [
                (6, "501", "photo"),
                (7, "502", "video"),
            ]
            assert payload["results"][0]["text"] == "Whole post caption"
            assert payload["results"][0]["forwarded"]["origin_status"] == "unknown"
            assert all(row["source"]["chat_id"] == "1" for row in payload["results"])
            assert payload["album"]["id"] == "901"
            assert payload["album"]["complete"] is True
    assert direct_audio["engines"] == []
    assert core_client.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["peer", "message"])
async def test_context_rejects_a_changed_central_source(
    owner_config, core_client, direct_audio, change
):
    path, _ = owner_config
    if change == "peer":
        direct_audio["message"].peer_id = types.PeerUser(2)
    else:
        direct_audio["message"].id = 8
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool(
                "get_message_context", {"chat_id": 1, "message_id": 7, "context_size": 0}
            )
            assert result.isError
            assert "synthetic transcript" not in _text(result)
    assert direct_audio["engines"] == []


def test_rich_post_keeps_missing_media_and_reports_incomplete_coverage():
    from datetime import datetime, timezone

    post = SimpleNamespace(
        id=9,
        date=datetime.now(timezone.utc),
        message="",
        sender=None,
        rich_message=SimpleNamespace(
            blocks=[SimpleNamespace(photo_id=1), SimpleNamespace(video_id=2)],
            photos=[SimpleNamespace(id=1)],
            documents=[],
        ),
    )
    record = messages.message_to_dict(post, chat_id=-1000000000010)
    assert record["rich_media"] == [{"type": "photo", "id": 1}, {"type": "video", "id": 2}]
    assert record["media_coverage"]["complete"] is False
    assert record["media_coverage"]["missing_media_ids"] == ["2"]
    assert record["source"]["link"].endswith("/c/10/9")


def test_long_post_reports_existing_text_truncation_explicitly():
    from datetime import datetime, timezone

    source = SimpleNamespace(
        id=4, date=datetime.now(timezone.utc), message="x" * 5000, sender=None
    )
    record = messages.message_to_dict(source, chat_id=1)
    assert record["text_coverage"] == {
        "complete": False,
        "characters_available": 5000,
        "characters_returned": 4096,
    }
    assert record["text"].endswith("[truncated]")


def test_private_context_keeps_large_rich_media_and_album_ids_exact(owner_config):
    from datetime import datetime, timezone

    path, _ = owner_config
    config = access.load_access_config(path)
    large_id = 9223372036854775806
    source = SimpleNamespace(
        id=4,
        date=datetime.now(timezone.utc),
        message="Post",
        sender=None,
        grouped_id=large_id,
        rich_message=SimpleNamespace(
            blocks=[SimpleNamespace(photo_id=large_id)],
            photos=[SimpleNamespace(id=large_id)],
            documents=[],
        ),
    )
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        record = messages.message_to_dict(source, chat_id=1)
    finally:
        access.current_connection.reset(token)
    assert record["rich_media"][0]["id"] == str(large_id)
    assert record["grouped_id"] == str(large_id)


@pytest.mark.parametrize("saved", [False, True])
def test_forward_author_does_not_invent_a_private_origin(saved):
    from datetime import datetime, timezone

    header = types.MessageFwdHeader(date=datetime.now(timezone.utc), from_id=types.PeerUser(42))
    if saved:
        header.saved_from_peer, header.saved_from_msg_id = types.PeerChat(9), 81
    source = SimpleNamespace(
        id=10,
        date=header.date,
        message="Forward",
        sender=None,
        fwd_from=header,
        forward=SimpleNamespace(chat_id=42, sender=SimpleNamespace(first_name="Author")),
    )
    forwarded = messages.message_to_dict(source, chat_id=1)["forwarded"]
    assert forwarded["origin_status"] == ("known" if saved else "unknown")
    if saved:
        assert forwarded["source_chat_id"] == -9
        assert forwarded["source_message_id"] == 81
    else:
        assert "source_chat_id" not in forwarded


def test_empty_forward_header_keeps_public_shape_and_private_unknown(owner_config):
    source = SimpleNamespace(
        id=10,
        date=None,
        message="Forward",
        sender=None,
        fwd_from=types.MessageFwdHeader(date=None),
    )
    assert messages.message_to_dict(source)["forwarded"] is True
    path, _ = owner_config
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        assert messages.message_to_dict(source)["forwarded"] == {"origin_status": "unknown"}
    finally:
        access.current_connection.reset(token)


def test_private_openai_default_preserves_generic_core_default(
    owner_config, direct_audio, monkeypatch
):
    path, _ = owner_config
    monkeypatch.delenv("TELEGRAM_TRANSCRIBE_OPENAI_MODEL")
    assert transcription.http_engine_config("openai").model == "whisper-1"
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        assert transcription.http_engine_config("openai").model == "gpt-transcribe"
    finally:
        access.current_connection.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "media,status,expected_source,expected_reason",
    [
        ("voice", 503, "telegram", None),
        ("video_note", 503, "telegram", None),
        ("audio", 503, "openai", "native_unsupported_media"),
        ("voice", 401, "openai", "provider_credentials"),
        ("voice", 413, "openai", "too_large"),
        ("voice", "timeout", "openai", "provider_outcome_unknown"),
    ],
)
async def test_private_provider_fallback_and_specific_failures(
    owner_config,
    core_client,
    direct_audio,
    monkeypatch,
    media,
    status,
    expected_source,
    expected_reason,
):
    path, _ = owner_config
    source = direct_audio["message"]
    if media == "voice":
        source.document.attributes = [types.DocumentAttributeAudio(duration=5, voice=True)]
    elif media == "video_note":
        source.document.attributes = [
            types.DocumentAttributeVideo(duration=5, w=32, h=32, round_message=True)
        ]
    posts, native, downloads = [], [], []

    async def download(*args, **kwargs):
        downloads.append(1)
        kwargs["file"].write(b"syntheticwav")
        return kwargs["file"]

    class Provider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            posts.append(url)
            if status == "timeout":
                raise httpx.ReadTimeout("secret-shaped-provider-details")
            return httpx.Response(status, request=httpx.Request("POST", url), json={})

    async def telegram(*args):
        native.append(1)
        return {"status": "ok", "text": "Native result"}

    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    # Use a separate HTTP module facade; the test's real MCP client uses httpx too.
    monkeypatch.setattr(
        transcription,
        "httpx",
        SimpleNamespace(
            AsyncClient=lambda **kwargs: Provider(),
            HTTPStatusError=httpx.HTTPStatusError,
            ConnectError=httpx.ConnectError,
        ),
    )
    monkeypatch.setattr(transcription, "_transcribe_via_telegram", telegram)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            payload = json.loads(_text(result)[_text(result).index("{") :])
            assert "secret-shaped" not in _text(result)
            assert payload["source"] == expected_source
            assert result.isError == (expected_reason is not None)
            if expected_reason is None:
                assert payload["complete"] is True
                assert payload["fallback_reason"] == "provider_unavailable"
                assert native == [1]
            else:
                assert payload["reason"] == expected_reason
                assert payload["complete"] is False
                assert native == []
    assert len(posts) == 1
    assert downloads == [1]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["endpoint", "key", "opt_in", "size"])
async def test_private_audio_preflight_never_downloads_or_calls_provider(
    owner_config, core_client, direct_audio, monkeypatch, change
):
    path, data = owner_config
    calls = []
    if change == "endpoint":
        monkeypatch.setenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "https://unapproved.invalid/v1")
    elif change == "key":
        monkeypatch.delenv("TELEGRAM_TRANSCRIBE_OPENAI_API_KEY")
    elif change == "opt_in":
        data["profiles"]["reader"]["external_asr"] = []
        _write_config(path, data)
    else:
        direct_audio["message"].document.size = 24_000_001

    async def forbidden(*args, **kwargs):
        calls.append("download")
        raise AssertionError("Provider and download must not be called")

    async def forbidden_provider(*args, **kwargs):
        calls.append("provider")
        raise AssertionError("Provider must not be called")

    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setattr(core_client, "download_media", forbidden, raising=False)
    monkeypatch.setattr(transcription, "_post_audio", forbidden_provider)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert result.isError, _text(result)
            if change == "size":
                assert "too_large" in _text(result)
    assert calls == []


@pytest.mark.asyncio
async def test_unknown_send_result_is_not_repeated_or_reconciled_without_read(
    owner_config, core_client, monkeypatch
):
    path, data = owner_config
    data["profiles"]["sender"]["read"] = []
    _write_config(path, data)

    async def unknown(entity, text, **kwargs):
        core_client.sent.append((entity.id, text, kwargs.get("reply_to")))
        raise TimeoutError("synthetic uncertain send")

    monkeypatch.setattr(core_client, "send_message", unknown)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_B) as session:
            result = await session.call_tool(
                "send_message", {"chat_id": 2, "message": "synthetic", "reply_to": 9}
            )
            assert result.isError
            assert "unknown" in _text(result)
            assert "Do not resend" in _text(result)
    assert core_client.sent == [(2, "synthetic", 9)]
    assert core_client.content_calls == 0


@pytest.mark.asyncio
async def test_cancelled_asr_releases_input_and_does_not_store_text(
    owner_config, core_client, direct_audio, monkeypatch, transcript_cache_dir
):
    path, _ = owner_config
    config = access.load_access_config(path)
    entered, closed = asyncio.Event(), asyncio.Event()

    async def download(*args, **kwargs):
        kwargs["file"].write(b"synthetic-input")
        return kwargs["file"]

    class Provider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.set()

        async def post(self, *args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    monkeypatch.setattr(
        transcription,
        "httpx",
        SimpleNamespace(
            AsyncClient=lambda **kwargs: Provider(),
            HTTPStatusError=httpx.HTTPStatusError,
            ConnectError=httpx.ConnectError,
        ),
    )
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        server = access.DotMCP(path, runtime.mcp)
        task = asyncio.create_task(
            server.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
        )
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed.is_set()
        assert transcription._INFLIGHT_LOCKS == {}
    finally:
        access.current_connection.reset(token)
    assert transcription.get_cached_transcript(1, 7) is None
    assert list(config.media_root.iterdir()) == []
    assert {item.name for item in transcript_cache_dir.iterdir()} == {"transcripts.db"}


@pytest.mark.asyncio
async def test_revocation_during_download_prevents_external_dispatch(
    owner_config, core_client, direct_audio, monkeypatch
):
    path, data = owner_config
    posts = []

    async def download(*args, **kwargs):
        data["profiles"]["reader"]["external_asr"] = []
        _write_config(path, data)
        kwargs["file"].write(b"synthetic-input")
        return kwargs["file"]

    async def forbidden(*args, **kwargs):
        posts.append(1)
        raise AssertionError("ASR dispatch after revocation")

    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    monkeypatch.setattr(transcription.httpx.AsyncClient, "post", forbidden)
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        with pytest.raises(runtime.ToolError):
            await access.DotMCP(path, runtime.mcp).call_tool(
                "transcribe_voice", {"chat_id": 1, "message_id": 7}
            )
    finally:
        access.current_connection.reset(token)
    assert posts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["attachment", "deleted"])
async def test_source_change_during_download_prevents_external_dispatch(
    owner_config, core_client, direct_audio, monkeypatch, change
):
    path, _ = owner_config
    posts = []

    async def download(*args, **kwargs):
        if change == "attachment":
            direct_audio["message"].document.id += 1
        else:
            direct_audio["message"] = None
        kwargs["file"].write(b"synthetic-input")
        return kwargs["file"]

    async def post(*args, **kwargs):
        posts.append(1)
        return httpx.Response(
            200,
            request=httpx.Request("POST", "https://api.openai.com/v1/audio/transcriptions"),
            json={"text": "Withdrawn source must not leave Telegram"},
        )

    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    monkeypatch.setattr(transcription.httpx.AsyncClient, "post", post)
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        with pytest.raises(runtime.ToolError):
            await access.DotMCP(path, runtime.mcp).call_tool(
                "transcribe_voice", {"chat_id": 1, "message_id": 7}
            )
    finally:
        access.current_connection.reset(token)
    assert posts == []
    assert transcription.get_cached_transcript(1, 7) is None


@pytest.mark.asyncio
async def test_real_sdk_concurrent_profiles_and_hidden_tools(owner_config, core_client):
    path, _ = owner_config
    async with _server(path) as (_, app):

        async def read(token, expected_id):
            async with _session(app, token) as session:
                tools = await session.list_tools()
                names = {tool.name for tool in tools.tools}
                assert ("send_message" in names) == (token == TOKEN_B)
                result = await session.call_tool("get_chats", {})
                assert not result.isError
                assert [item["chat_id"] for item in json.loads(_text(result))["results"]] == [
                    expected_id
                ]
                for hidden in (
                    "transcribe_voice",
                    "export_unread_messages",
                    "delete_message",
                    "download_media",
                ):
                    denied = await session.call_tool(hidden, {})
                    assert denied.isError
                spoof = await session.call_tool("get_chats", {"profile": "sender"})
                assert spoof.isError
                assert "Hidden two" not in _text(spoof)

        await asyncio.gather(read(TOKEN_A, 1), read(TOKEN_B, 2))
    assert core_client.sent == []
    assert access.current_connection.get() is None


@pytest.mark.asyncio
async def test_send_scope_is_separate_from_read_and_bool_is_rejected(owner_config, core_client):
    path, data = owner_config
    data["profiles"]["sender"]["read"] = []
    _write_config(path, data)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as reader:
            denied = await reader.call_tool("send_message", {"chat_id": 1, "message": "synthetic"})
            assert denied.isError
            malformed = await reader.call_tool("get_messages", {"chat_id": True})
            assert malformed.isError
        async with _session(app, TOKEN_B) as sender:
            tools = (await sender.list_tools()).tools
            assert [tool.name for tool in tools] == ["send_message"]
            assert tools[0].annotations.idempotentHint is False
            assert "Send a message to a specific chat" in tools[0].description
            assert "already-authorized" in tools[0].description
            result = await sender.call_tool(
                "send_message", {"chat_id": 2, "message": "synthetic", "reply_to": 9}
            )
            assert not result.isError, _text(result)
            assert "message_id=55" in _text(result)
            denied = await sender.call_tool("send_message", {"chat_id": 1, "message": "synthetic"})
            assert denied.isError
    assert core_client.sent == [(2, "synthetic", 9)]


@pytest.mark.asyncio
async def test_revocation_after_send_discloses_unknown_outcome(owner_config, core_client):
    path, data = owner_config
    original = core_client.send_message

    async def sent_before_revocation(entity, text, **kwargs):
        result = await original(entity, text, **kwargs)
        data["profiles"]["sender"]["send"] = []
        _write_config(path, data)
        return result

    core_client.send_message = sent_before_revocation
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_B) as sender:
            result = await sender.call_tool("send_message", {"chat_id": 2, "message": "synthetic"})
            assert result.isError
            assert "outcome may be unknown" in _text(result)
            assert "Do not resend" in _text(result)
    assert core_client.sent == [(2, "synthetic", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize("midflight", [False, True])
async def test_revocation_withholds_sdk_result(owner_config, core_client, midflight):
    path, data = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as reader:
            first = await reader.call_tool("get_chats", {})
            assert not first.isError

            def revoke():
                data["profiles"]["reader"]["read"] = []
                _write_config(path, data)

            if midflight:
                core_client.before_return = revoke
            else:
                revoke()
            result = await reader.call_tool("get_chats", {})
            assert result.isError
            assert "Visible one" not in _text(result)


@pytest.mark.asyncio
async def test_canonical_source_and_alias_suggestions_do_not_leak(
    owner_config, core_client, monkeypatch
):
    path, _ = owner_config
    monkeypatch.setattr(
        runtime,
        "load_aliases",
        lambda: {
            "visible": {"id": 1, "name": "Visible one"},
            "hidden": {"id": 2, "name": "Hidden two"},
        },
    )
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as reader:
            unknown = await reader.call_tool("get_messages", {"chat_id": "hidde"})
            assert "Hidden two" not in _text(unknown)
            assert '"hidden"' not in _text(unknown)
            hidden = await reader.call_tool("get_messages", {"chat_id": "hidden"})
            assert hidden.isError
            assert "'2'" not in _text(hidden)
            core_client.resolve_as = types.InputPeerChannel(channel_id=1, access_hash=0)
            mismatch = await reader.call_tool("get_messages", {"chat_id": 1})
            assert mismatch.isError
            assert core_client.content_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"x-dot-backend-token": "wrong"},
        [("x-dot-backend-token", TOKEN_A), ("x-dot-backend-token", TOKEN_B)],
        {"x-dot-backend-token": TOKEN_A + ", " + TOKEN_B},
    ],
)
async def test_invalid_auth_covers_every_http_path(owner_config, core_client, headers):
    path, _ = owner_config
    async with _server(path) as (_, app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://127.0.0.1:8765"
        ) as client:
            for method, target in (
                ("GET", "/mcp"),
                ("OPTIONS", "/mcp"),
                ("POST", "/mcp"),
                ("DELETE", "/mcp"),
                ("GET", "/original/1"),
            ):
                response = await client.request(method, target, headers=headers)
                assert response.status_code == 401
                assert TOKEN_A not in response.text
    assert core_client.dialog_calls == 0


def test_default_scope_is_saved_messages_and_permissions_are_immutable(owner_config):
    path, data = owner_config
    data["profiles"] = {"dot": {"token": TOKEN_A}}
    _write_config(path, data)
    config = access.load_access_config(path)
    profile = config.profiles[0]
    assert profile.read == frozenset({1})
    assert profile.send == profile.attachments == profile.external_asr == frozenset()
    assert TOKEN_A not in repr(config)
    with pytest.raises(AttributeError):
        profile.read = None


@pytest.mark.parametrize(
    "patch",
    [
        {"read": True},
        {"read": [True]},
        {"read": ["1"]},
        {"read": "*"},
        {"external_asr": True},
        {"unknown": [1]},
        {"token": "short"},
    ],
)
def test_invalid_configuration_fails_closed(owner_config, patch):
    path, data = owner_config
    data["profiles"]["reader"].update(patch)
    _write_config(path, data)
    with pytest.raises(access.AccessDenied):
        access.load_access_config(path)


def test_media_root_cannot_contain_credentials_source_or_session(owner_config, tmp_path):
    path, data = owner_config
    for root in (tmp_path, Path(runtime.PROJECT_ROOT), path):
        data["media_root"] = str(root)
        _write_config(path, data)
        with pytest.raises(access.AccessDenied):
            access.load_access_config(path)
    media = tmp_path / "media"
    with pytest.raises(ValueError, match="overlaps"):
        access.validate_media_root(media, path, media / "owner.session")
    link = tmp_path / "linked"
    link.symlink_to(media, target_is_directory=True)
    with pytest.raises(ValueError):
        access.validate_media_root(link, path)


@pytest.mark.asyncio
async def test_nullable_list_and_operational_failures_use_sdk_errors(owner_config, core_client):
    path, _ = owner_config
    received_limits = []
    original = core_client.get_dialogs

    async def dialogs(**kwargs):
        received_limits.append(kwargs.get("limit"))
        return await original(**kwargs)

    core_client.get_dialogs = dialogs
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as reader:
            result = await reader.call_tool("list_chats", {"unread_only": True})
            assert not result.isError
            assert received_limits == [None]
            for args in ({"with_about": True}, {"with_about": True, "limit": 101}, {"limit": 0}):
                result = await reader.call_tool("list_chats", args)
                assert result.isError
            assert received_limits == [None]

            async def failing(**kwargs):
                raise runtime.FloodWaitError(request=None, capture=7)

            core_client.get_dialogs = failing
            result = await reader.call_tool("list_chats", {})
            assert result.isError
            assert "7 seconds" in _text(result)
            assert "incomplete" in _text(result).lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["not json", "{}", "[]", '{"results": {}}'])
async def test_list_wrapper_rejects_malformed_or_plain_core_failure(value):
    async def original(**kwargs):
        return value

    with pytest.raises(runtime.ToolError):
        await access.DotMCP._nullable_list_chats(original)()


@pytest.mark.asyncio
async def test_explicit_all_still_disables_implicit_warming_and_guessing(
    owner_config, core_client
):
    path, data = owner_config
    data["profiles"]["reader"]["read"] = "all"
    _write_config(path, data)
    looked_up = []

    async def missing(identifier):
        looked_up.append(identifier)
        raise ValueError("synthetic missing source")

    core_client.get_entity = missing
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[0])
    )
    try:
        with pytest.raises(ValueError):
            await runtime.resolve_entity(123, core_client)
    finally:
        access.current_connection.reset(token)
    assert looked_up == [123, 123]
    assert core_client.dialog_calls == 0


@pytest.mark.asyncio
async def test_cancelled_tool_restores_operation_and_resolution_context(owner_config, core_client):
    path, _ = owner_config
    config = access.load_access_config(path)
    server = access.DotMCP(path, runtime.mcp)
    entered = asyncio.Event()

    async def blocked(entity, text, **kwargs):
        entered.set()
        await asyncio.Future()

    core_client.send_message = blocked

    async def request():
        token = access.current_connection.set(
            access.AuthenticatedConnection(path, config, config.profiles[1])
        )
        try:
            await server.call_tool("send_message", {"chat_id": 2, "message": "synthetic"})
        except asyncio.CancelledError:
            assert access.current_operation.get() == "read"
            assert runtime._chat_resolution_targets.get() is None
            raise
        finally:
            access.current_connection.reset(token)

    task = asyncio.create_task(request())
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert core_client.sent == []


def test_legacy_ceiling_narrows_each_independent_operation(owner_config, monkeypatch):
    path, _ = owner_config
    config = access.load_access_config(path)
    token = access.current_connection.set(
        access.AuthenticatedConnection(path, config, config.profiles[1])
    )
    monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "1")
    operation = access.current_operation.set("send")
    try:
        assert runtime.get_effective_allowed_chat_ids() == set()
        monkeypatch.setenv("TELEGRAM_ALLOWED_CHAT_IDS", "1,2")
        assert runtime.get_effective_allowed_chat_ids() == {2}
    finally:
        access.current_operation.reset(operation)
        access.current_connection.reset(token)


@pytest.mark.parametrize(
    "change_during_startup,entrypoint_in_media", [(False, False), (True, False), (False, True)]
)
def test_wrapper_uses_installed_layout_and_removes_legacy_handlers(
    owner_config, core_client, monkeypatch, tmp_path, change_during_startup, entrypoint_in_media
):
    from deploy.dot import dot_server
    from telegram_mcp import runner
    from telegram_mcp.tools import events

    path, data = owner_config
    monkeypatch.setattr(dot_server.os, "environ", dot_server.os.environ.copy())
    for key in list(runtime.os.environ):
        if key.startswith("TELEGRAM_SESSION_") or key in (
            "MCP_TRANSPORT",
            "TELEGRAM_LEGACY_EVENTS",
            "TELEGRAM_EVENT_FEED",
            "TELEGRAM_SERVER_ROOTS_ONLY",
        ):
            monkeypatch.delenv(key, raising=False)
    session = tmp_path / "owner.session"
    session.touch(mode=0o600)
    monkeypatch.setenv("DOT_ACCESS_CONFIG", str(path))
    monkeypatch.setenv("TELEGRAM_SESSION_NAME", str(session))
    monkeypatch.setattr(dot_server, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        dot_server,
        "__file__",
        str(tmp_path / "media" / "dot_server.py") if entrypoint_in_media else "/app/dot_server.py",
    )
    monkeypatch.setattr(events, "clients", {"default": core_client})
    monkeypatch.setattr(events, "_feed_task", None)
    monkeypatch.setattr(runner, "mcp", runner.mcp)
    monkeypatch.setattr(runtime, "SERVER_ALLOWED_ROOTS", [])

    async def neighbor(event):
        pass

    core_client.handlers = [
        (events._on_new_incoming, None),
        (events._message_event_handler("default"), None),
        (neighbor, None),
    ]
    if entrypoint_in_media:
        with pytest.raises(ValueError, match="entrypoint directory"):
            dot_server.prepare()
        return
    if change_during_startup:
        original = dot_server.DotMCP

        def changed(path, source):
            data["owner_id"] = 3
            _write_config(path, data)
            return original(path, source)

        monkeypatch.setattr(dot_server, "DotMCP", changed)
        with pytest.raises(ValueError, match="changed during startup"):
            dot_server.prepare()
        assert runtime.SERVER_ALLOWED_ROOTS == []
        return
    assert dot_server.prepare() is runner
    assert core_client.handlers == [(neighbor, None)]
    assert runtime.SERVER_ALLOWED_ROOTS == [tmp_path / "media"]
    assert runner.mcp._dot_owner_id == 1
    assert runtime.os.environ["TELEGRAM_LEGACY_EVENTS"] == "0"


@pytest.mark.parametrize(
    "setting,value",
    [
        ("TELEGRAM_SESSION_STRING", "synthetic"),
        ("TELEGRAM_SESSION_NAME_WORK", "synthetic"),
        ("TELEGRAM_SESSION_LOCK", "shared"),
        ("TELEGRAM_TRANSCRIBE", "auto"),
        ("TELEGRAM_EVENT_FEED", "1"),
        ("TELEGRAM_LEGACY_EVENTS", "1"),
    ],
)
def test_wrapper_rejects_unsafe_explicit_settings(owner_config, monkeypatch, setting, value):
    from deploy.dot import dot_server

    path, _ = owner_config
    monkeypatch.setattr(dot_server.os, "environ", dot_server.os.environ.copy())
    for key in (
        "MCP_TRANSPORT",
        "TELEGRAM_SESSION_LOCK",
        "TELEGRAM_LEGACY_EVENTS",
        "TELEGRAM_EVENT_FEED",
        "TELEGRAM_TRANSCRIBE",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(dot_server, "load_dotenv", lambda: None)
    monkeypatch.setenv("DOT_ACCESS_CONFIG", str(path))
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError):
        dot_server.prepare()


@pytest.mark.asyncio
async def test_dot_runner_uses_authenticated_transport(owner_config, monkeypatch):
    from telegram_mcp.transport import http

    path, _ = owner_config
    server = access.DotMCP(path, runtime.mcp)
    called = []

    async def serve(instance):
        called.append(instance)

    monkeypatch.setattr(http, "run_streamable_http_async", serve)
    await server.run_streamable_http_async()
    assert called == [server]


@pytest.mark.asyncio
async def test_real_loopback_sdk_requests_are_authenticated_and_scoped(owner_config, core_client):
    path, _ = owner_config
    server = access.DotMCP(path, runtime.mcp)
    app = create_streamable_http_app(server)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = listener.getsockname()[1]
    worker = uvicorn.Server(uvicorn.Config(app, log_level="critical", lifespan="on"))
    task = asyncio.create_task(worker.serve(sockets=[listener]))
    try:
        for _ in range(100):
            if worker.started or task.done():
                break
            await asyncio.sleep(0.01)
        assert worker.started
        url = f"http://127.0.0.1:{port}/mcp"
        async with httpx.AsyncClient(trust_env=False) as client:
            assert (await client.get(url)).status_code == 401
        async with httpx.AsyncClient(
            headers={"x-dot-backend-token": TOKEN_A}, trust_env=False
        ) as client:
            async with streamable_http_client(url, http_client=client) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    result = await session.call_tool("get_chats", {})
                    assert not result.isError
                    assert "Visible one" in _text(result)
                    assert "Hidden two" not in _text(result)
    finally:
        worker.should_exit = True
        await asyncio.wait_for(task, 3)
        listener.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("setting", ["owner_id", "media_root"])
async def test_installation_identity_changes_require_restart(
    owner_config, core_client, setting, tmp_path
):
    path, data = owner_config
    async with _server(path) as (_, app):
        if setting == "owner_id":
            data["owner_id"] = 3
        else:
            root = tmp_path / "changed-media"
            root.mkdir(mode=0o700)
            data["media_root"] = str(root)
        _write_config(path, data)
        async with _session(app, TOKEN_A) as reader:
            result = await reader.call_tool("get_chats", {})
            assert result.isError
            assert "Visible one" not in _text(result)
    assert core_client.dialog_calls == 0


@pytest.mark.asyncio
async def test_sdk_preserves_core_timeout_and_untrusted_audience(
    owner_config, core_client, monkeypatch
):
    path, _ = owner_config
    monkeypatch.setenv("TELEGRAM_TOOL_TIMEOUT_SECONDS", "0.02")
    original = core_client.get_dialogs
    cancelled = []

    async def blocked(**kwargs):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    core_client.get_dialogs = blocked
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as reader:
            result = await reader.call_tool("get_chats", {})
            assert result.isError
            assert "GEN-TIMEOUT" in _text(result)
            assert result.content[0].annotations.audience == ["user"]
            assert cancelled == [True]
            core_client.get_dialogs = original
            monkeypatch.setenv("TELEGRAM_TOOL_TIMEOUT_SECONDS", "1")
            result = await reader.call_tool("get_chats", {})
            assert not result.isError
            assert result.content[0].annotations.audience == ["user"]
