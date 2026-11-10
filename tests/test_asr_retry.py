"""Synthetic checks of Dot's authorized two-attempt provider budget."""

import asyncio
import io

import pytest

from telegram_mcp import transcription as audio
from telegram_mcp.access import AccessDenied


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first", ["provider_unavailable", "provider_outcome_unknown", "provider_credentials"]
)
async def test_one_retry_rewinds_and_revalidates(monkeypatch, first):
    events = []

    async def validate():
        events.append("validate")

    async def post(stream, *args):
        events.append(stream.read())
        return (
            {"status": "error", "reason": first}
            if events.count(b"synthetic") == 1
            else {"status": "ok", "text": "done"}
        )

    monkeypatch.setattr(audio, "_post_audio", post)
    result = await audio._post_audio_with_retry(
        io.BytesIO(b"synthetic"), None, "a.wav", "audio/wav", validate
    )
    assert result["status"] == "ok"
    assert events == [b"synthetic", "validate", b"synthetic"]


@pytest.mark.asyncio
async def test_second_provider_error_exhausts_budget(monkeypatch):
    calls = []

    async def validate():
        calls.append("validate")

    async def post(*args):
        calls.append("post")
        return {"status": "error", "reason": "provider_outcome_unknown"}

    monkeypatch.setattr(audio, "_post_audio", post)
    result = await audio._post_audio_with_retry(
        io.BytesIO(b"synthetic"), None, "a.wav", "audio/wav", validate
    )
    assert calls == ["post", "validate", "post"]
    assert result["attempts"] == 2 and result["retry_exhausted"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", [AccessDenied, asyncio.CancelledError])
async def test_stop_before_retry_prevents_second_upload(monkeypatch, stop):
    posts = []

    async def validate():
        raise stop()

    async def post(*args):
        posts.append(1)
        return {"status": "error", "reason": "provider_outcome_unknown"}

    monkeypatch.setattr(audio, "_post_audio", post)
    with pytest.raises(stop):
        await audio._post_audio_with_retry(
            io.BytesIO(b"synthetic"), None, "a.wav", "audio/wav", validate
        )
    assert posts == [1]


@pytest.mark.asyncio
async def test_local_size_error_has_no_retry(monkeypatch):
    async def post(*args):
        return {"status": "error", "reason": "too_large"}

    async def validate():
        raise AssertionError("local failure must not retry")

    monkeypatch.setattr(audio, "_post_audio", post)
    result = await audio._post_audio_with_retry(
        io.BytesIO(b"synthetic"), None, "a.wav", "audio/wav", validate
    )
    assert result["reason"] == "too_large"
