"""Synthetic streamed audio input and provider boundaries."""

import asyncio
import io
from types import SimpleNamespace

import httpx
import pytest

from telegram_mcp import transcription


@pytest.fixture
def provider(monkeypatch):
    calls = []

    class Provider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            audio = kwargs["files"]["file"][1]
            assert not isinstance(audio, bytes)
            calls.append(audio.read())
            return httpx.Response(
                200, request=httpx.Request("POST", url), json={"text": "complete"}
            )

    monkeypatch.setattr(transcription.httpx, "AsyncClient", lambda **kwargs: Provider())
    return calls


def config(limit=8):
    return transcription.HttpEngineConfig(
        engine="openai",
        label="OpenAI",
        url=transcription.OPENAI_TRANSCRIBE_URL,
        api_key="synthetic",
        model="gpt-transcribe",
        response_format="json",
        max_bytes=limit,
        max_mb_env="TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB",
        timeout=1,
    )


def message():
    return SimpleNamespace(file=SimpleNamespace(ext="wav", mime_type="audio/wav"))


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [7, 8, 9])
async def test_actual_stream_bytes_enforce_under_at_over_limit(provider, size):
    streams = []

    async def download(msg, *, file):
        streams.append(file)
        for _ in range(size):
            file.write(b"x")
        return file

    result = await transcription._transcribe_via_openai_compatible(
        SimpleNamespace(download_media=download), message(), config()
    )
    assert streams[0].closed
    if size <= 8:
        assert result["status"] == "ok"
        assert provider == [b"x" * size]
    else:
        assert result["reason"] == "too_large"
        assert provider == []


@pytest.mark.asyncio
async def test_over_limit_chunk_is_stopped_before_disk_write(provider):
    streams = []

    async def download(msg, *, file):
        streams.append(file)
        file.write(b"x" * 4)
        with pytest.raises(Exception):
            file.write(b"x" * 5)
        assert file.tell() == 4
        raise OSError("synthetic disk-full detail")

    result = await transcription._transcribe_via_openai_compatible(
        SimpleNamespace(download_media=download), message(), config()
    )
    assert result["reason"] == "download_failed"
    assert "synthetic" not in result["error"]
    assert provider == []
    assert streams[0].closed


@pytest.mark.asyncio
async def test_cancelled_download_closes_temporary_audio(provider):
    entered, streams = asyncio.Event(), []

    async def download(msg, *, file):
        streams.append(file)
        file.write(b"audio")
        entered.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(
        transcription._transcribe_via_openai_compatible(
            SimpleNamespace(download_media=download), message(), config()
        )
    )
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert streams[0].closed
    assert provider == []


@pytest.mark.asyncio
async def test_uncertain_provider_post_is_not_repeated(monkeypatch):
    streams, posts = [], []

    async def download(msg, *, file):
        streams.append(file)
        file.write(b"audio")
        return file

    class Provider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, *args, **kwargs):
            posts.append(1)
            raise httpx.ReadTimeout("synthetic provider detail")

    monkeypatch.setattr(transcription.httpx, "AsyncClient", lambda **kwargs: Provider())
    result = await transcription._transcribe_via_openai_compatible(
        SimpleNamespace(download_media=download), message(), config()
    )
    assert result["reason"] == "provider_outcome_unknown"
    assert posts == [1]
    assert streams[0].closed


@pytest.mark.asyncio
async def test_disk_full_during_buffer_flush_is_sanitized_and_closed(provider, monkeypatch):
    class FullDisk(io.BytesIO):
        def write(self, data):
            raise OSError("synthetic disk path and private details")

    raw = FullDisk()
    monkeypatch.setattr(transcription.tempfile, "TemporaryFile", lambda **kwargs: raw)

    async def download(msg, *, file):
        file.write(b"audio")
        return file

    result = await transcription._transcribe_via_openai_compatible(
        SimpleNamespace(download_media=download), message(), config()
    )
    assert result["reason"] == "download_failed"
    assert "synthetic" not in result["error"]
    assert raw.closed
    assert provider == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["no_result", "short_download"])
async def test_partial_download_never_reaches_provider(provider, failure):
    msg = message()
    if failure == "short_download":
        msg.file.size = 8
    streams = []

    async def download(msg, *, file):
        streams.append(file)
        file.write(b"audio")
        return file if failure == "short_download" else None

    result = await transcription._transcribe_via_openai_compatible(
        SimpleNamespace(download_media=download), msg, config()
    )
    assert result["reason"] == "download_incomplete"
    assert streams[0].closed
    assert provider == []


@pytest.mark.asyncio
async def test_actual_provider_part_bytes_are_checked_before_post(provider):
    result = await transcription._post_audio(
        io.BytesIO(b"x" * 9), config(), "part.wav", "audio/wav"
    )
    assert result["reason"] == "too_large"
    assert provider == []


@pytest.mark.asyncio
async def test_malformed_success_response_is_an_unknown_outcome(monkeypatch):
    class Provider:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kwargs):
            return httpx.Response(
                200, request=httpx.Request("POST", url), json=["private details"]
            )

    monkeypatch.setattr(transcription.httpx, "AsyncClient", lambda **kwargs: Provider())
    result = await transcription._post_audio(
        io.BytesIO(b"audio"), config(), "part.wav", "audio/wav"
    )
    assert result["reason"] == "provider_outcome_unknown"
    assert "private details" not in result["error"]
