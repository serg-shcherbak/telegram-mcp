"""Public private-tool start/poll/cancel and disclosure boundaries, with synthetic audio."""

import asyncio
import json
import sqlite3
import time
import wave

import pytest

from telegram_mcp import transcription, transcription_jobs as jobs
from test_dot_profile import (
    TOKEN_A,
    TOKEN_B,
    _server,
    _session,
    _text,
    _write_config,
    owner_config,
    core_client,
    direct_audio,
)


@pytest.fixture
def long_audio(owner_config, core_client, direct_audio, monkeypatch, tmp_path):
    direct_audio["message"].document.mime_type = "audio/aiff"
    direct_audio["message"].document.attributes[0].duration = 601
    monkeypatch.setattr(transcription, "transcribe", direct_audio["real_transcribe"])
    monkeypatch.setenv("TELEGRAM_AUDIO_SCRATCH_DIR", str(tmp_path / "audio"))
    monkeypatch.setattr(jobs, "converter_available", lambda root: True)
    state = {"posts": [], "downloads": [], "gate": None, "between": None}

    async def download(msg, *, file):
        if state.get("before_download"):
            state["before_download"]()
        state["downloads"].append(msg.id)
        file.write(b"syntheticwav")
        return file

    async def parts(root, job_id, limit):
        for index in range(2):
            path = root / job_id / "part.wav"
            with wave.open(str(path), "wb") as audio:
                audio.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
                audio.writeframes(bytes([index, 0]) * 4)
            yield {"index": index, "start_sample": index * 4, "frames": 4}, path
            path.unlink(missing_ok=True)
            if state["between"]:
                state["between"]()

    async def post(data, cfg, filename, mime):
        state["posts"].append(data.read())
        if state["gate"]:
            await state["gate"].wait()
        return {"status": "ok", "text": f"part {len(state['posts'])}", "lang": "en"}

    monkeypatch.setattr(core_client, "download_media", download, raising=False)
    monkeypatch.setattr(jobs.converter, "receive_parts", parts)
    monkeypatch.setattr(transcription, "_post_audio", post)
    return state


async def call(session, **extra):
    result = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7, **extra})
    text = _text(result)
    return result, json.loads(text[text.index("{") :])


async def settled(job_id):
    task = jobs._TASKS.get(job_id)
    if task is not None:
        await asyncio.wait_for(asyncio.shield(task), 2)


async def posted(state):
    async def wait():
        while not state["posts"]:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), 2)


@pytest.mark.asyncio
async def test_public_long_audio_starts_polls_and_preserves_order(owner_config, long_audio):
    path, _ = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            result, pending = await call(session)
            assert not result.isError
            assert pending["status"] == "pending" and pending["complete"] is False
            await settled(pending["job_id"])
            result, complete = await call(session)
            assert not result.isError
            assert complete["text"] == "part 1\npart 2"
            assert complete["complete"] is True
            assert complete["sample_rate"] == 16000
            assert [p["start_sample"] for p in complete["parts"]] == [0, 4]
            assert [p["end_sample"] for p in complete["parts"]] == [4, 8]
    assert len(long_audio["posts"]) == 2
    assert long_audio["downloads"] == [7]
    assert not list(jobs.scratch_root().glob("[a-f0-9]" * 32))


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["cancel", "deadline"])
async def test_cancel_or_deadline_reports_uncertain_range_and_never_reposts(
    owner_config, long_audio, monkeypatch, finish
):
    path, _ = owner_config
    long_audio["gate"] = asyncio.Event()
    if finish == "deadline":
        monkeypatch.setattr(jobs, "MAX_JOB_SECONDS", 1)
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await posted(long_audio)
            if finish == "deadline":
                await settled(pending["job_id"])
            _, cancelled = await call(session, cancel=finish == "cancel")
            assert cancelled["complete"] is False
            assert cancelled["unknown_ranges"] == [{"start_sample": 0, "end_sample": 4}]
            _, again = await call(session)
            assert again["reason"] == "provider_outcome_unknown"
    assert len(long_audio["posts"]) == 1
    assert not (jobs.scratch_root() / pending["job_id"]).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["rights", "source"])
async def test_next_part_and_result_withheld_after_revocation_or_source_change(
    owner_config, direct_audio, long_audio, change
):
    path, config = owner_config

    def changed():
        if change == "rights":
            config["profiles"]["reader"]["external_asr"] = []
            _write_config(path, config)
        else:
            direct_audio["message"].document.id += 1

    long_audio["between"] = changed
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await settled(pending["job_id"])
            if change == "source":
                direct_audio["message"].document.id -= 1
            result = await session.call_tool("transcribe_voice", {"chat_id": 1, "message_id": 7})
            assert result.isError
            assert "part 1" not in _text(result)
    assert len(long_audio["posts"]) == 1


@pytest.mark.asyncio
async def test_another_profile_cannot_poll_pending_text_or_start_second_download(
    owner_config, long_audio
):
    path, config = owner_config
    config["profiles"]["sender"]["read"] = [1]
    config["profiles"]["sender"]["external_asr"] = [1]
    _write_config(path, config)
    long_audio["gate"] = asyncio.Event()
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as first:
            _, pending = await call(first)
            async with _session(app, TOKEN_B) as other:
                _, blocked = await call(other)
                assert blocked["reason"] == "asr_busy"
                assert "parts" not in blocked and "job_id" not in blocked
            await call(first, cancel=True)
    assert len(long_audio["downloads"]) <= 1


@pytest.mark.asyncio
async def test_restart_exposes_unknown_checkpoint_without_new_upload(owner_config, long_audio):
    path, _ = owner_config
    long_audio["gate"] = asyncio.Event()
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await posted(long_audio)
            await call(session, cancel=True)
            database = transcription.cache_dir() / "audio-jobs.db"
            with sqlite3.connect(database) as connection:
                key, payload = connection.execute("SELECT key,payload FROM jobs").fetchone()
                state = json.loads(payload)
                state.update(status="pending", reason="pending", unknown_ranges=[])
                connection.execute(
                    "UPDATE jobs SET payload=? WHERE key=?", (json.dumps(state), key)
                )
            jobs._MEMORY.clear()
            folder = jobs.scratch_root() / pending["job_id"]
            folder.mkdir()
            (folder / "source.bin").write_bytes(b"interrupted synthetic source")
            _, interrupted = await call(session)
            assert interrupted["reason"] == "provider_outcome_unknown"
            assert interrupted["unknown_ranges"] == [{"start_sample": 0, "end_sample": 4}]
            assert not folder.exists()
    assert len(long_audio["posts"]) == 1


@pytest.mark.asyncio
async def test_retention_zero_keeps_active_text_only_until_result_is_retrieved(
    owner_config, long_audio, monkeypatch
):
    monkeypatch.setenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", "0")
    path, _ = owner_config
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await settled(pending["job_id"])
            _, result = await call(session)
            assert result["text"] == "part 1\npart 2"
            with sqlite3.connect(transcription.cache_dir() / "audio-jobs.db") as connection:
                payload = connection.execute("SELECT payload FROM jobs").fetchone()[0]
            assert "part 1" not in payload and "part 2" not in payload
            _, second = await call(session)
            assert second["reason"] == "result_not_retained"
    assert len(long_audio["posts"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("duration", [601, None])
async def test_low_bitrate_long_or_unknown_duration_uses_jobs_below_upload_limit(
    owner_config, direct_audio, long_audio, duration
):
    path, _ = owner_config
    direct_audio["message"].document.mime_type = "audio/mpeg"
    direct_audio["message"].document.attributes[0].duration = duration
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            assert pending["job_id"] and pending["complete"] is False
            await settled(pending["job_id"])
            _, result = await call(session)
            assert result["complete"] is True
    assert len(long_audio["posts"]) == 2


@pytest.mark.asyncio
async def test_expired_unknown_job_keeps_no_text_and_does_not_recharge(
    owner_config, long_audio, monkeypatch
):
    path, _ = owner_config
    long_audio["gate"] = asyncio.Event()
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            await call(session)
            await posted(long_audio)
            await call(session, cancel=True)
            with sqlite3.connect(transcription.cache_dir() / "audio-jobs.db") as connection:
                key, payload = connection.execute("SELECT key,payload FROM jobs").fetchone()
                state = json.loads(payload)
                state["created"] = time.time() - 8 * 86400
                state["parts"] = [{"index": 0, "text": "expired private transcript"}]
                connection.execute(
                    "UPDATE jobs SET payload=?,created=? WHERE key=?",
                    (json.dumps(state), state["created"], key),
                )
            jobs._MEMORY.clear()
            monkeypatch.setattr(jobs, "converter_available", lambda root: False)
            _, result = await call(session)
            assert result["reason"] == "provider_outcome_unknown"
            assert "expired private transcript" not in json.dumps(result)
    assert len(long_audio["posts"]) == 1


@pytest.mark.asyncio
async def test_restart_recovers_other_source_before_new_download(
    owner_config, direct_audio, long_audio
):
    path, _ = owner_config
    long_audio["gate"] = asyncio.Event()
    async with _server(path) as (_, app):
        async with _session(app, TOKEN_A) as session:
            _, pending = await call(session)
            await posted(long_audio)
            await call(session, cancel=True)
            with sqlite3.connect(transcription.cache_dir() / "audio-jobs.db") as connection:
                key, payload = connection.execute("SELECT key,payload FROM jobs").fetchone()
                state = json.loads(payload)
                state.update(status="pending", reason="pending", unknown_ranges=[])
                connection.execute(
                    "UPDATE jobs SET payload=? WHERE key=?", (json.dumps(state), key)
                )
            jobs._MEMORY.clear()
            orphan = jobs.scratch_root() / pending["job_id"]
            orphan.mkdir()
            (orphan / "source.bin").write_bytes(b"old interrupted source")
            sentinel = jobs.scratch_root() / ("b" * 32)
            sentinel.mkdir()
            (sentinel / "keep.txt").write_text("unrelated existing directory")

            def before_download():
                assert not orphan.exists(), "old source survived into the next source download"

            long_audio["before_download"] = before_download
            long_audio["gate"] = None
            direct_audio["message"].document.id += 1
            _, fresh = await call(session)
            await settled(fresh["job_id"])
            _, result = await call(session)
            assert result["complete"] is True
            assert not orphan.exists()
            assert (sentinel / "keep.txt").read_text() == "unrelated existing directory"
