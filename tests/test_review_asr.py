"""Synthetic regressions for the final review's paid-request and cache failures."""

import asyncio
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from telegram_mcp import transcription
from test_dot_profile import (
    TOKEN_A,
    _server,
    _session,
    _text,
    owner_config,
    core_client,
    direct_audio,
)
from test_transcription_jobs import long_audio, call, posted, settled


@pytest.mark.asyncio
@pytest.mark.parametrize("retention", ["0", "604800"])
async def test_unknown_short_post_survives_waiting_callers_and_restart(
    owner_config, core_client, direct_audio, monkeypatch, retention
):
    monkeypatch.setenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", retention)
    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    posts = []

    async def download(msg, *, file):
        file.write(b"syntheticwav")
        return file

    async def post(*args):
        posts.append(True)
        await asyncio.sleep(0)
        return {"status": "error", "reason": "provider_outcome_unknown"}

    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    monkeypatch.setattr(transcription, "_post_audio", post)
    async with _server(owner_config[0]) as (_, app):
        async with _session(app, TOKEN_A) as session:
            results = await asyncio.gather(call(session), call(session))
            assert all(value[1]["reason"] == "provider_outcome_unknown" for value in results)
            transcription._INFLIGHT_LOCKS.clear()
            _, again = await call(session)
            assert again["reason"] == "provider_outcome_unknown"
    assert len(posts) == 1


@pytest.mark.asyncio
async def test_private_cache_cannot_purge_or_reuse_generic_transcripts(
    owner_config, direct_audio, transcript_cache_dir, monkeypatch
):
    monkeypatch.delenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", raising=False)
    transcription.save_transcript(1, 7, "openai", "existing generic transcript")
    with (
        closing(sqlite3.connect(transcript_cache_dir / "transcripts.db")) as connection,
        connection,
    ):
        connection.execute("UPDATE transcripts SET expires_at=0, created_at='2020-01-01'")
    async with _server(owner_config[0]) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, result = await call(session)
            assert result["text"] == "synthetic transcript" and result["cached"] is False
    assert transcription.get_cached_transcript(1, 7)["text"] == "existing generic transcript"


@pytest.mark.asyncio
@pytest.mark.parametrize("age_days", [1, 30])
async def test_generic_upgrade_preserves_legacy_text_and_cache_hit(
    transcript_cache_dir, monkeypatch, age_days
):
    monkeypatch.delenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", raising=False)
    created = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    with closing(sqlite3.connect(transcription._cache_db_path())) as connection, connection:
        connection.execute(
            "CREATE TABLE transcripts (chat_id INTEGER, message_id INTEGER, source TEXT, "
            "text TEXT, duration INTEGER, lang TEXT, created_at TEXT, "
            "PRIMARY KEY (chat_id, message_id, source))"
        )
        connection.execute(
            "INSERT INTO transcripts VALUES (1,7,'groq','retained legacy text',5,'en',?)",
            (created,),
        )

    async def no_provider(*args):
        pytest.fail("A migrated cached transcript must not trigger another paid request")

    monkeypatch.setattr(transcription, "transcribe", no_provider)
    result = await transcription.transcribe_cached(None, None, SimpleNamespace(id=7), "groq", 1)
    assert result["text"] == "retained legacy text" and result["cached"] is True


@pytest.mark.asyncio
async def test_long_provider_failure_preserves_paid_parts_without_native_fallback(
    owner_config, direct_audio, long_audio, monkeypatch
):
    monkeypatch.setenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", "0")
    direct_audio["message"].document.attributes[0].voice = True
    posts = []

    async def post(*args):
        posts.append(True)
        if len(posts) == 1:
            return {"status": "ok", "text": "completed paid part"}
        return {"status": "error", "reason": "provider_unavailable"}

    async def no_fallback(*args):
        pytest.fail("Native fallback must not replace a partial job result")

    monkeypatch.setattr(transcription, "_post_audio", post)
    monkeypatch.setattr(transcription, "_transcribe_via_telegram", no_fallback)
    async with _server(owner_config[0]) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await settled(pending["job_id"])
            _, partial = await call(session)
            assert partial["reason"] == "provider_unavailable"
            assert partial["parts"][0]["text"] == "completed paid part"
            assert partial["complete"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["provider", "cancel", "restart"])
async def test_unknown_and_unprocessed_ranges_do_not_overlap(
    owner_config, long_audio, monkeypatch, finish
):
    if finish == "provider":

        async def post(*args):
            return {"status": "error", "reason": "provider_outcome_unknown"}

        monkeypatch.setattr(transcription, "_post_audio", post)
    else:
        long_audio["gate"] = asyncio.Event()
    async with _server(owner_config[0]) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            if finish == "provider":
                await settled(pending["job_id"])
            else:
                await posted(long_audio)
                await call(session, cancel=True)
            if finish == "restart":
                from telegram_mcp import transcription_jobs as jobs
                import json

                with (
                    closing(
                        sqlite3.connect(transcription.cache_dir() / "audio-jobs.db")
                    ) as connection,
                    connection,
                ):
                    key, payload = connection.execute("SELECT key,payload FROM jobs").fetchone()
                    state = json.loads(payload)
                    state.update(status="pending", unknown_ranges=[])
                    connection.execute(
                        "UPDATE jobs SET payload=? WHERE key=?", (json.dumps(state), key)
                    )
                jobs._MEMORY.clear()
            _, result = await call(session)
            assert result["unknown_ranges"] == [{"start_sample": 0, "end_sample": 4}]
            assert result["unprocessed_ranges"] == [{"start_sample": 4, "end_sample": None}]


@pytest.mark.asyncio
async def test_dot_catalog_explains_polling_and_cancellation(owner_config, direct_audio):
    async with _server(owner_config[0]) as (_, app):
        async with _session(app, TOKEN_A) as session:
            tools = (await session.list_tools()).tools
            description = next(
                tool.description for tool in tools if tool.name == "transcribe_voice"
            )
            assert "same chat_id and message_id" in description
            assert "poll" in description and "cancel=true" in description
