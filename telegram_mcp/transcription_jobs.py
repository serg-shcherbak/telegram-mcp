import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
from uuid import uuid4

from telegram_mcp import audio_converter as converter
from telegram_mcp.access import AccessDenied, current_connection

MAX_JOB_SECONDS = 7200
_TASKS = {}
_MEMORY = {}
_ACTIVE = None


def scratch_root():
    from telegram_mcp import transcription as audio

    root = Path(os.getenv("TELEGRAM_AUDIO_SCRATCH_DIR") or audio.cache_dir().parent / "audio")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(root, 0o700)
    return root


def converter_available(root):
    return (root / "converter.sock").is_socket()


def busy():
    return _ACTIVE is not None


def _key(identity):
    connection = current_connection.get()
    return hashlib.sha256(
        json.dumps([connection.profile.name, identity], ensure_ascii=False).encode()
    ).hexdigest()


def _without_text(state):
    state = dict(state)
    state.pop("text", None)
    state["parts"] = [{k: v for k, v in part.items() if k != "text"} for part in state["parts"]]
    return state


@contextmanager
def _connect():
    from telegram_mcp import transcription as audio

    path = audio.cache_dir() / "audio-jobs.db"
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    os.close(descriptor)
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA secure_delete=ON")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS jobs (key TEXT PRIMARY KEY, payload TEXT NOT NULL, created REAL NOT NULL)"
        )
        retention = audio.retention_seconds()
        for key, payload, created in connection.execute(
            "SELECT key,payload,created FROM jobs"
        ).fetchall():
            if retention == 0 or created + retention <= time.time():
                state = json.loads(payload)
                stripped = json.dumps(_without_text(state), ensure_ascii=False)
                if stripped != payload:
                    connection.execute("UPDATE jobs SET payload=? WHERE key=?", (stripped, key))
                if retention:
                    _MEMORY.pop(state["job_id"], None)
        connection.commit()
        with connection:
            yield connection
    finally:
        connection.close()


def _save(key, state):
    from telegram_mcp import transcription as audio

    _MEMORY[state["job_id"]] = state
    retention = audio.retention_seconds()
    persisted = (
        state if retention and state["created"] + retention > time.time() else _without_text(state)
    )
    with _connect() as connection:
        connection.execute(
            "INSERT INTO jobs VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload",
            (key, json.dumps(persisted, ensure_ascii=False), state["created"]),
        )


def existing(identity):
    from telegram_mcp import transcription as audio

    if not (audio.cache_dir() / "audio-jobs.db").exists():
        return False
    with _connect() as connection:
        return (
            connection.execute("SELECT 1 FROM jobs WHERE key=?", (_key(identity),)).fetchone()
            is not None
        )


def recover():
    """Before another source starts, discard every orphan source and preserve POST uncertainty."""
    from telegram_mcp import transcription as audio

    root = scratch_root()
    owned = set()
    if (audio.cache_dir() / "audio-jobs.db").exists():
        with _connect() as connection:
            rows = connection.execute("SELECT key,payload FROM jobs").fetchall()
        for key, payload in rows:
            state = json.loads(payload)
            if not isinstance(state.get("job_id"), str) or not re.fullmatch(
                "[a-f0-9]{32}", state["job_id"]
            ):
                raise ValueError("Invalid local audio job identity")
            owned.add(state["job_id"])
            if state["status"] == "pending" and state["job_id"] not in _TASKS:
                if state.get("posting") and state["posting"] not in state["unknown_ranges"]:
                    state["unknown_ranges"].append(state["posting"])
                state.update(
                    status="error",
                    complete=False,
                    reason="provider_outcome_unknown" if state.get("posting") else "interrupted",
                )
                _save(key, state)
    for job_id in owned - _TASKS.keys():
        folder = root / job_id
        if folder.exists() or folder.is_symlink():
            if folder.is_symlink():
                folder.unlink()
            elif folder.is_dir():
                shutil.rmtree(folder)


def _public(state):
    result = {
        key: state[key]
        for key in (
            "status",
            "job_id",
            "complete",
            "parts",
            "unknown_ranges",
            "unprocessed_ranges",
            "reason",
        )
    }
    result["cached"] = state["status"] == "ok"
    result["source"] = "openai"
    result["sample_rate"] = converter.SAMPLE_RATE
    result["duration"] = state["end_sample"] / converter.SAMPLE_RATE if state["complete"] else None
    result["limits"] = {
        "source_bytes": converter.MAX_SOURCE_BYTES,
        "part_bytes": converter.MAX_PART_BYTES,
        "duration_seconds": converter.MAX_SAMPLES / converter.SAMPLE_RATE,
        "job_seconds": MAX_JOB_SECONDS,
    }
    if state.get("text"):
        result["text"] = state["text"]
    return result


async def request(cl, entity, msg, cfg, identity, *, cancel=False):
    global _ACTIVE
    from telegram_mcp import transcription as audio
    from telegram_mcp.runtime import get_marked_id

    chat_id = get_marked_id(msg.peer_id)
    await audio._revalidate_audio_source(cl, entity, chat_id, msg.id, cfg.engine, identity)
    recover()
    key, root = _key(identity), scratch_root()
    with _connect() as connection:
        row = connection.execute("SELECT payload FROM jobs WHERE key=?", (key,)).fetchone()
    state = json.loads(row[0]) if row else None
    if state:
        state = _MEMORY.get(state["job_id"], state)
        task = _TASKS.get(state["job_id"])
        if cancel and task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                if state["status"] == "pending":
                    state.update(status="error", reason="cancelled")
                    _save(key, state)
        if state["status"] == "ok" and not state.get("text"):
            state.update(status="error", complete=False, reason="result_not_retained")
        await audio._revalidate_audio_source(cl, entity, chat_id, msg.id, cfg.engine, identity)
        result = _public(state)
        if state["status"] != "pending":
            _MEMORY.pop(state["job_id"], None)
        return result
    if cancel:
        return {"status": "error", "reason": "no_active_job", "complete": False}
    if busy() or audio._AUDIO_LOCK.locked():
        return {"status": "error", "reason": "asr_busy", "complete": False}
    if not converter_available(root):
        return {
            "status": "error",
            "reason": "too_large",
            "complete": False,
            "error": "This audio requires the isolated converter, which is unavailable.",
        }
    state = {
        "status": "pending",
        "job_id": uuid4().hex,
        "complete": False,
        "parts": [],
        "unknown_ranges": [],
        "unprocessed_ranges": [{"start_sample": 0, "end_sample": None}],
        "reason": "pending",
        "created": time.time(),
        "end_sample": 0,
        "posting": None,
    }
    _MEMORY.clear()
    _save(key, state)
    _ACTIVE = state["job_id"]
    task = asyncio.create_task(_run(key, state, root, cl, entity, msg, cfg, identity, chat_id))
    _TASKS[state["job_id"]] = task

    def finished(done):
        global _ACTIVE
        _TASKS.pop(state["job_id"], None)
        if _ACTIVE == state["job_id"]:
            _ACTIVE = None
        if not done.cancelled():
            done.exception()
        if audio.retention_seconds():
            _MEMORY.pop(state["job_id"], None)

    task.add_done_callback(finished)
    return _public(state)


async def _run(key, state, root, cl, entity, msg, cfg, identity, chat_id):
    from telegram_mcp import transcription as audio

    folder, data = root / state["job_id"], None

    async def validate():
        await audio._revalidate_audio_source(cl, entity, chat_id, msg.id, cfg.engine, identity)

    try:
        async with asyncio.timeout(MAX_JOB_SECONDS), audio._AUDIO_LOCK:
            folder.mkdir(mode=0o700)
            await validate()
            data, error = await audio._download_audio(
                cl, msg, converter.MAX_SOURCE_BYTES, cfg, directory=folder
            )
            if error:
                state.update(status="error", reason=error.get("reason", "download_failed"))
                return
            data.close()
            data = None
            await validate()
            parts = converter.receive_parts(
                root, state["job_id"], min(cfg.max_bytes, converter.MAX_PART_BYTES)
            )
            try:
                async for part, path in parts:
                    await validate()
                    source_range = {
                        "start_sample": part["start_sample"],
                        "end_sample": part["start_sample"] + part["frames"],
                    }
                    state["posting"] = source_range
                    _save(key, state)
                    with path.open("rb") as stream:
                        result = await audio._post_audio(stream, cfg, "part.wav", "audio/wav")
                    await validate()
                    if result["status"] != "ok":
                        state.update(
                            status="error", reason=result.get("reason", "provider_rejected")
                        )
                        if result.get("reason") == "provider_outcome_unknown":
                            state["unknown_ranges"].append(source_range)
                        return
                    state["parts"].append(
                        dict(
                            part,
                            end_sample=source_range["end_sample"],
                            status="complete",
                            text=result["text"],
                        )
                    )
                    state["posting"] = None
                    state["end_sample"] = source_range["end_sample"]
                    state["unprocessed_ranges"] = [
                        {"start_sample": state["end_sample"], "end_sample": None}
                    ]
                    _save(key, state)
            finally:
                await parts.aclose()
            await validate()
            if not state["parts"]:
                state.update(status="error", reason="unsupported_audio")
                return
            state.update(
                status="ok",
                complete=True,
                reason=None,
                unprocessed_ranges=[],
                text="\n".join(part["text"] for part in state["parts"]),
            )
    except AccessDenied:
        state.update(status="error", reason="access_changed", parts=[], complete=False)
    except converter.ConversionError as error:
        state.update(status="error", reason=error.reason, complete=False)
    except (asyncio.CancelledError, TimeoutError) as error:
        if state["posting"]:
            state["unknown_ranges"].append(state["posting"])
        state.update(
            status="error",
            reason=(
                "provider_outcome_unknown"
                if state["posting"]
                else "job_timeout" if isinstance(error, TimeoutError) else "cancelled"
            ),
            complete=False,
        )
    except Exception:
        state.update(status="error", reason="conversion_failed", complete=False)
    finally:
        if data is not None:
            data.close()
        shutil.rmtree(folder, ignore_errors=True)
        _save(key, state)
