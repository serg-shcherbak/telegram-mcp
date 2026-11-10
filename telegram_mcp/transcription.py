"""Voice/video-note transcription: engines, SQLite cache, and batch budget.

Four engines behind one interface (see ``transcribe``):

* ``telegram`` - native Premium ``messages.transcribeAudio``. Free, but drops
  the last speech segment in roughly 2 of 3 real recordings (proven with
  per-segment timestamps against a local Whisper run on 2026-08-20 - see
  ``~/Workspace/AI Playground/docs/2026-08-20-telegram-voice-transcripts.md``).
  Async: a long recording comes back ``pending=True`` and must be polled.
* ``groq`` - Groq-hosted ``whisper-large-v3-turbo``. Does not drop the tail,
  but costs a download+upload per voice message and sends the audio to a
  third party. Primary engine by owner decision (2026-08-20); ``telegram``
  is the free/private fallback.
* ``openai`` - any OpenAI-compatible ``/audio/transcriptions`` endpoint
  (OpenAI itself, a self-hosted achetronic/parakeet or speaches server,
  LocalAI, a vLLM whisper, ...) at a configured base URL, with an optional
  API key.
* ``whisper`` - a local faster-whisper model loaded in this process. Never
  leaves the machine; needs the ``whisper`` extra installed.

``groq`` and ``openai`` share one HTTP path
(:func:`_transcribe_via_openai_compatible`); only their settings differ.

Every transcript is a machine reading, not a verbatim quote (proper names and
punctuation drift under both engines) - callers must surface ``source``
alongside ``text`` rather than presenting it as exact speech.
"""

import asyncio
import importlib.util
import io
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from telethon.tl import functions

from telegram_mcp.runtime import account_is_premium, get_marked_id, is_premium_rpc_error
from telegram_mcp.access import AccessDenied, current_connection, current_operation

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_TRANSCRIBE_MODES = {"off", "on-demand", "auto"}
ENGINES = {"telegram", "groq", "openai", "whisper"}

GROQ_TRANSCRIBE_URL = "https://api.groq.com/openai/v1/audio/transcriptions"
GROQ_MODEL = "whisper-large-v3-turbo"

OPENAI_DEFAULT_MODEL = "whisper-1"
OPENAI_TRANSCRIBE_URL = "https://api.openai.com/v1/audio/transcriptions"

WHISPER_DEFAULT_MODEL = "small"

# Self-hosted servers on a CPU can take well over a minute for a long voice
# note, so the HTTP engines get more room than a hosted API would need.
HTTP_DEFAULT_TIMEOUT_SECONDS = 120.0

# Groq rejects an upload above its per-file limit, and the limit depends on
# the account tier (25 MB on the free tier at the time of writing, more on
# paid ones). Checked locally so an oversized recording fails with an
# actionable message instead of a 413 from the API after a full download.
GROQ_DEFAULT_MAX_MB = 25.0


def transcribe_mode() -> str:
    """``TELEGRAM_TRANSCRIBE``: off | on-demand | auto (default on-demand).

    ``off`` disables the dedicated tool and all auto-mixing. ``on-demand``
    (default) keeps the dedicated tool and fills cache hits into listings,
    but never spends a batch call fetching a new one. ``auto`` additionally
    prefetches missing transcripts into listings, bounded by
    :class:`TranscribeBudget`.
    """
    raw = os.getenv("TELEGRAM_TRANSCRIBE", "on-demand").strip().lower()
    if raw not in _TRANSCRIBE_MODES:
        accepted = ", ".join(sorted(_TRANSCRIBE_MODES))
        raise SystemExit(f"Invalid TELEGRAM_TRANSCRIBE '{raw}'. Expected one of: {accepted}.")
    return raw


def default_engine() -> str:
    """``TELEGRAM_TRANSCRIBE_ENGINE``: telegram | groq | openai | whisper
    (default groq).

    Groq is the primary engine (owner decision 2026-08-20: native Telegram
    transcription drops the last speech segment in ~2 of 3 recordings).
    Override to ``telegram`` for chats that should never leave the server,
    or when GROQ_API_KEY / quota is unavailable, or to one of the
    self-hosted engines. Per-call ``engine=`` on transcribe_voice always
    takes precedence over this default.
    """
    raw = os.getenv("TELEGRAM_TRANSCRIBE_ENGINE", "groq").strip().lower()
    if raw not in ENGINES:
        accepted = ", ".join(sorted(ENGINES))
        raise SystemExit(
            f"Invalid TELEGRAM_TRANSCRIBE_ENGINE '{raw}'. Expected one of: {accepted}."
        )
    return raw


def validate_transcription_config() -> None:
    """Fail loudly at startup on a bad toggle, matching TELEGRAM_EXPOSED_TOOLS."""
    transcribe_mode()
    default_engine()


def transcribe_language() -> Optional[str]:
    """``TELEGRAM_TRANSCRIBE_LANGUAGE``: ISO-639-1 hint (``nl``, ``en``, ...).

    Unset lets whisper-style engines detect the language themselves. Some
    OpenAI-compatible servers do not detect it: achetronic/parakeet assumes
    ``en`` when no language is sent, so non-English chats need this set.
    """
    raw = os.getenv("TELEGRAM_TRANSCRIBE_LANGUAGE", "").strip().lower()
    return raw or None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    try:
        value = float(raw) if raw else default
    except ValueError:
        value = default
    return value if value > 0 else default


def _endpoint(base: str) -> str:
    """Accept either a base URL (``https://api.openai.com/v1``, like the
    OpenAI SDKs' base_url) or the full transcriptions URL."""
    url = base.strip().rstrip("/")
    if url.endswith("/audio/transcriptions"):
        return url
    return url + "/audio/transcriptions"


@dataclass(frozen=True)
class HttpEngineConfig:
    """Settings for one OpenAI-compatible transcription endpoint."""

    engine: str
    label: str
    url: Optional[str]
    api_key: Optional[str]
    model: str
    response_format: str
    max_bytes: Optional[int]
    max_mb_env: str
    timeout: float


def _http_timeout() -> float:
    return _env_float("TELEGRAM_TRANSCRIBE_TIMEOUT", HTTP_DEFAULT_TIMEOUT_SECONDS)


def http_engine_config(engine: str) -> Optional[HttpEngineConfig]:
    """Config for an HTTP engine, or None for telegram/whisper."""
    if engine == "groq":
        return HttpEngineConfig(
            engine="groq",
            label="Groq",
            url=GROQ_TRANSCRIBE_URL,
            api_key=os.getenv("GROQ_API_KEY") or None,
            model=GROQ_MODEL,
            response_format="verbose_json",
            max_bytes=groq_max_upload_bytes(),
            max_mb_env="TELEGRAM_TRANSCRIBE_GROQ_MAX_MB",
            timeout=_http_timeout(),
        )
    if engine == "openai":
        base = os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "").strip()
        return HttpEngineConfig(
            engine="openai",
            label="OpenAI-compatible",
            url=_endpoint(base) if base else None,
            api_key=os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_API_KEY") or None,
            model=os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_MODEL", "").strip()
            or (
                "gpt-transcribe" if current_connection.get() is not None else OPENAI_DEFAULT_MODEL
            ),
            # "json" is the one format every compatible server and every
            # OpenAI model (gpt-4o-transcribe included) accepts; verbose_json
            # is whisper-only.
            response_format="json",
            max_bytes=(
                min(
                    24_000_000,
                    int(_env_float("TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB", 25.0) * 1048576),
                )
                if current_connection.get() is not None
                else int(_env_float("TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB", 25.0) * 1048576)
            ),
            max_mb_env="TELEGRAM_TRANSCRIBE_OPENAI_MAX_MB",
            timeout=_http_timeout(),
        )
    return None


def whisper_available() -> bool:
    return importlib.util.find_spec("faster_whisper") is not None


def engine_config_error(engine: str) -> Optional[str]:
    """Why ``engine`` cannot run with the current settings, or None.

    Checked before any download, so a misconfigured engine costs nothing.
    """
    if current_connection.get() is not None:
        cfg = http_engine_config(engine)
        if engine != "openai" or cfg is None or cfg.url != OPENAI_TRANSCRIBE_URL:
            return "Dot ASR requires the official OpenAI transcription endpoint."
        if not cfg.api_key:
            return "TELEGRAM_TRANSCRIBE_OPENAI_API_KEY is not configured."
    if engine == "groq" and not os.getenv("GROQ_API_KEY"):
        return (
            "GROQ_API_KEY is not configured on this server. "
            "Use another engine or set GROQ_API_KEY."
        )
    if engine == "openai" and not os.getenv("TELEGRAM_TRANSCRIBE_OPENAI_URL", "").strip():
        return (
            "TELEGRAM_TRANSCRIBE_OPENAI_URL is not configured on this server. Set it to the "
            "base URL of an OpenAI-compatible API (e.g. https://api.openai.com/v1)."
        )
    if engine == "whisper" and not whisper_available():
        return (
            "faster-whisper is not installed on this server. Install it with "
            "pip install 'telegram-mcp[whisper]' (or pip install faster-whisper)."
        )
    return None


# ---------------------------------------------------------------------------
# SQLite cache
# ---------------------------------------------------------------------------
#
# This is a personal-chat transcript store in plaintext on the VPS - mode 600,
# its own directory (not the session/app directory), mounted as a volume so a
# rebuild doesn't silently drop it (docker-compose.yml has no volume by
# default).


def groq_max_upload_bytes() -> int:
    """Upload ceiling for the groq engine, in bytes.

    ``TELEGRAM_TRANSCRIBE_GROQ_MAX_MB`` raises or lowers it: a paid Groq tier
    accepts larger files than the free one, and hardcoding the free-tier number
    would refuse recordings the account can actually transcribe.
    """
    limit_mb = _env_float("TELEGRAM_TRANSCRIBE_GROQ_MAX_MB", GROQ_DEFAULT_MAX_MB)
    return int(limit_mb * 1024 * 1024)


def _too_large(size: int, limit: int, cfg: HttpEngineConfig) -> dict:
    return {
        "status": "error",
        "reason": "too_large",
        "error": (
            f"recording is {size / 1048576:.1f} MB, above the "
            f"{limit / 1048576:.0f} MB {cfg.label} upload limit. Transcribe it with "
            f"engine='telegram' or engine='whisper', or raise {cfg.max_mb_env} if the "
            f"{cfg.label} endpoint accepts larger uploads."
        ),
    }


DEFAULT_CACHE_DIR = "data/transcripts"


def retention_seconds() -> float:
    """Plaintext transcript retention: seven days; zero disables storage and purges on access."""
    try:
        value = float(os.getenv("TELEGRAM_TRANSCRIPT_RETENTION_SECONDS", "604800"))
    except ValueError:
        value = -1
    if not math.isfinite(value) or value < 0:
        raise ValueError("Invalid transcript retention setting")
    return value


def check_transcript_access(chat_id: int, *, operation=False) -> None:
    connection = current_connection.get()
    if connection is None:
        return
    profile = connection.revalidate()
    if operation and current_operation.get() != "external_asr":
        raise AccessDenied("An explicit transcription operation is required")
    if any(
        scope is not None and chat_id not in scope
        for scope in (profile.read, profile.external_asr)
    ):
        raise AccessDenied("Read and external ASR permission are required for this source")
    from telegram_mcp.runtime import check_chat_access

    if check_chat_access(chat_id):
        raise AccessDenied("Transcription source is not permitted")


def transcript_identity(msg, chat_id: int, engine: str, cl=None) -> str:
    """Identity comes only from the fetched source and server-owned provider settings."""
    from telegram_mcp.runtime import clients

    connection = current_connection.get()
    cl = cl or getattr(msg, "_client", None)
    account_id = getattr(getattr(cl, "_mb_entity_cache", None), "self_id", None)
    account = account_id or next(
        (name for name, client in clients.items() if client is cl), "default"
    )
    document = getattr(msg, "document", None)
    file = getattr(msg, "file", None)
    cfg = http_engine_config(engine)
    material = {
        "account": account,
        "owner": connection.config.owner_id if connection is not None else None,
        "peer": chat_id,
        "message": msg.id,
        "attachment": getattr(document, "id", None),
        "size": getattr(file, "size", None),
        "mime": getattr(file, "mime_type", None),
        "duration": voice_duration(msg),
        "edited": str(getattr(msg, "edit_date", None)),
        "engine": engine,
        "provider": cfg.url if cfg else engine,
        "model": cfg.model if cfg else _whisper_settings() if engine == "whisper" else None,
        "format": cfg.response_format if cfg else None,
        "language": transcribe_language(),
    }
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


async def _revalidate_audio_source(cl, entity, chat_id, message_id, engine, identity):
    if current_connection.get() is None:
        return
    check_transcript_access(chat_id, operation=True)
    current = await cl.get_messages(entity, ids=message_id)
    if (
        not current
        or current.id != message_id
        or get_marked_id(current.peer_id) != chat_id
        or transcript_identity(current, chat_id, engine, cl) != identity
    ):
        raise AccessDenied("Audio source changed during transcription; result withheld")
    check_transcript_access(chat_id, operation=True)


def cache_dir() -> Path:
    raw = os.getenv("TELEGRAM_TRANSCRIPT_CACHE_DIR")
    if raw:
        d = Path(raw)
        d.mkdir(parents=True, exist_ok=True)
    else:
        # The default is relative to the working directory, which is wherever the MCP client
        # started the server: often "/" or another read-only folder when the installed
        # `telegram-mcp` script is used. Fall back to a per-user cache instead of failing
        # every listing that contains a voice message.
        d = Path(DEFAULT_CACHE_DIR)
        try:
            d.mkdir(parents=True, exist_ok=True)
        except OSError:
            d = Path.home() / ".cache" / "telegram-mcp" / "transcripts"
            d.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(d, 0o700)
    except OSError:
        pass
    return d


def _cache_db_path() -> Path:
    return cache_dir() / "transcripts.db"


def _connect() -> sqlite3.Connection:
    path = _cache_db_path()
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA secure_delete = ON")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transcripts (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            text TEXT NOT NULL,
            duration INTEGER,
            lang TEXT,
            created_at TEXT NOT NULL,
            identity TEXT NOT NULL DEFAULT '',
            expires_at REAL NOT NULL,
            PRIMARY KEY (chat_id, message_id, source, identity)
        )
        """)
    _migrate_source_into_key(conn)
    conn.execute(
        "UPDATE transcripts SET expires_at = "
        "COALESCE(CAST(strftime('%s', created_at) AS INTEGER), 0) + :retention "
        "WHERE expires_at > COALESCE(CAST(strftime('%s', created_at) AS INTEGER), 0) + :retention",
        {"retention": retention_seconds()},
    )
    conn.execute("DELETE FROM transcripts WHERE expires_at <= ?", (time.time(),))
    if retention_seconds() == 0:
        conn.execute("DELETE FROM transcripts")
    conn.commit()
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return conn


def _migrate_source_into_key(conn: sqlite3.Connection) -> None:
    """Older builds keyed the cache on (chat_id, message_id) alone, so a cheap
    telegram transcript permanently shadowed the groq one - including for
    callers that asked for groq explicitly. Rebuild such a table in place."""
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'transcripts'"
    ).fetchone()
    if not row or not row[0]:
        return
    if "PRIMARY KEY (chat_id, message_id, source, identity)" in row[0]:
        return
    conn.executescript("""
        BEGIN IMMEDIATE;
        ALTER TABLE transcripts RENAME TO transcripts_legacy;
        CREATE TABLE transcripts (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            text TEXT NOT NULL,
            duration INTEGER,
            lang TEXT,
            created_at TEXT NOT NULL,
            identity TEXT NOT NULL DEFAULT '',
            expires_at REAL NOT NULL,
            PRIMARY KEY (chat_id, message_id, source, identity)
        );
        INSERT OR IGNORE INTO transcripts
            (chat_id, message_id, source, text, duration, lang, created_at, expires_at)
        SELECT chat_id, message_id, source, text, duration, lang, created_at,
            COALESCE(CAST(strftime('%s', created_at) AS INTEGER), 0) + 604800
        FROM transcripts_legacy;
        DROP TABLE transcripts_legacy;
        COMMIT;
        """)


def get_cached_transcript(
    chat_id: int, message_id: int, source: Optional[str] = None, *, identity: Optional[str] = None
) -> Optional[dict]:
    """Cached transcript for a message.

    ``source`` pins the engine: asking for groq must never be answered with a
    telegram transcript, because the native engine drops the recording's last
    segment and the loss is invisible in the text. Callers that only want to
    display whatever exists (listings, backfill skip checks) pass None and get
    the default engine's row when there is one, any row otherwise.
    """
    check_transcript_access(chat_id)
    if current_connection.get() is not None and identity is None:
        raise AccessDenied("A fresh source identity is required for cached transcripts")
    conn = _connect()
    try:
        identity_clause = " AND identity = ?" if identity is not None else ""
        parameters = (identity,) if identity is not None else ()
        if source is not None:
            row = conn.execute(
                "SELECT source, text, duration, lang, created_at FROM transcripts "
                "WHERE chat_id = ? AND message_id = ? AND source = ?"
                + identity_clause
                + " ORDER BY created_at DESC LIMIT 1",
                (chat_id, message_id, source) + parameters,
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT source, text, duration, lang, created_at FROM transcripts "
                "WHERE chat_id = ? AND message_id = ?" + identity_clause + " "
                "ORDER BY source = ? DESC, created_at DESC LIMIT 1",
                (chat_id, message_id) + parameters + (default_engine(),),
            ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    check_transcript_access(chat_id)
    source, text, duration, lang, created_at = row
    return {
        "source": source,
        "text": text,
        "duration": duration,
        "lang": lang,
        "created_at": created_at,
    }


def save_transcript(
    chat_id: int,
    message_id: int,
    source: str,
    text: str,
    *,
    duration: Optional[int] = None,
    lang: Optional[str] = None,
    identity: str = "",
) -> None:
    check_transcript_access(chat_id)
    conn = _connect()
    try:
        ttl = retention_seconds()
        if ttl == 0:
            return
        conn.execute(
            "INSERT OR REPLACE INTO transcripts "
            "(chat_id, message_id, source, text, duration, lang, created_at, identity, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                chat_id,
                message_id,
                source,
                text,
                duration,
                lang,
                datetime.now(timezone.utc).isoformat(),
                identity,
                time.time() + ttl,
            ),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Voice/video-note detection and formatting helpers
# ---------------------------------------------------------------------------


def is_transcribable(msg) -> bool:
    return any(getattr(msg, name, None) is not None for name in ("voice", "video_note", "audio"))


def voice_duration(msg) -> Optional[int]:
    """Seconds, from the already-fetched message - no extra API call."""
    f = getattr(msg, "file", None)
    if f is None:
        return None
    d = getattr(f, "duration", None)
    return int(d) if d is not None else None


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return "?:??"
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


# ---------------------------------------------------------------------------
# Engines
# ---------------------------------------------------------------------------

_POLL_ATTEMPTS = 10
_POLL_INTERVAL_SECONDS = 2.0


async def _transcribe_via_telegram(cl, entity, msg) -> dict:
    """Native Premium transcription. Polls while pending=True - proven live on
    2026-08-20: an 83s recording matured after 3 polls (~6.6s); without
    polling, long voice messages come back empty."""
    if getattr(msg, "voice", None) is None and getattr(msg, "video_note", None) is None:
        return {
            "status": "error",
            "reason": "native_unsupported_media",
            "error": "Native Telegram ASR supports only voice and video notes.",
        }
    if current_connection.get() is not None:
        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
    if not await account_is_premium(cl):
        return {"status": "premium_required"}
    peer = await cl.get_input_entity(entity)
    if current_connection.get() is not None:
        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
    try:
        result = await cl(functions.messages.TranscribeAudioRequest(peer=peer, msg_id=msg.id))
    except Exception as e:
        if is_premium_rpc_error(e):
            return {"status": "premium_required"}
        return {
            "status": "error",
            "reason": "native_failed",
            "error": "Native Telegram transcription failed.",
        }

    attempts = 0
    while getattr(result, "pending", False) and attempts < _POLL_ATTEMPTS:
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
        attempts += 1
        try:
            if current_connection.get() is not None:
                check_transcript_access(get_marked_id(msg.peer_id), operation=True)
            result = await cl(functions.messages.TranscribeAudioRequest(peer=peer, msg_id=msg.id))
        except AccessDenied:
            raise
        except Exception:
            return {
                "status": "error",
                "reason": "native_failed",
                "error": "Native Telegram transcription polling failed.",
            }

    if getattr(result, "pending", False):
        return {"status": "pending"}
    return {"status": "ok", "text": result.text}


class _AudioLimitExceeded(Exception):
    def __init__(self, size):
        self.size = size


class _AudioFile(io.BufferedRandom):
    def __init__(self, limit, directory=None):
        if directory is None:
            raw = tempfile.TemporaryFile(mode="w+b", buffering=0)
        else:
            descriptor = os.open(
                directory / "source.bin", os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600
            )
            raw = os.fdopen(descriptor, "w+b", buffering=0)
        super().__init__(raw)
        self.limit = limit

    def write(self, data):
        size = self.tell() + len(data)
        if self.limit is not None and size > self.limit:
            raise _AudioLimitExceeded(size)
        return super().write(data)

    def close(self):
        try:
            super().close()
        except OSError:
            # Buffered close still closes the raw file when its flush fails.
            pass


async def _download_audio(cl, msg, limit: Optional[int], cfg=None, *, directory=None):
    """Download to a private, automatically deleted file; caller closes success."""
    connection = current_connection.get()
    if connection is not None:
        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
    declared = getattr(getattr(msg, "file", None), "size", None)
    if limit is not None and isinstance(declared, int) and declared > limit:
        return None, _too_large(declared, limit, cfg)
    data = None
    ready = False
    try:
        data = _AudioFile(limit, directory)
        downloaded = await cl.download_media(msg, file=data)
        if not data.tell():
            return None, {"status": "error", "error": "empty media download"}
        if connection is not None:
            check_transcript_access(get_marked_id(msg.peer_id), operation=True)
        if not downloaded or (isinstance(declared, int) and data.tell() < declared):
            return None, {
                "status": "error",
                "reason": "download_incomplete",
                "error": "Audio download is incomplete; no audio was sent to ASR.",
            }
        data.seek(0)
        ready = True
        return data, None
    except _AudioLimitExceeded as error:
        return None, _too_large(error.size, limit, cfg)
    except AccessDenied:
        raise
    except Exception:
        return None, {
            "status": "error",
            "reason": "download_failed",
            "error": "Audio download failed.",
        }
    finally:
        if data is not None and not ready:
            data.close()


def _upload_name(msg) -> tuple:
    file_attr = getattr(msg, "file", None)
    ext = (getattr(file_attr, "ext", None) or "ogg").lstrip(".")
    # Telegram voice notes carry 'oga' (Opus-in-Ogg), which Groq (and OpenAI)
    # reject with 400 unsupported_audio_format. Same bytes, named .ogg.
    if ext.lower() == "oga":
        ext = "ogg"
    mime = getattr(file_attr, "mime_type", None) or "audio/ogg"
    return f"voice.{ext}", mime


async def _transcribe_via_openai_compatible(
    cl, msg, cfg: HttpEngineConfig, *, entity=None
) -> dict:
    if not cfg.url:
        return {"status": "error", "error": engine_config_error(cfg.engine)}

    source = None
    if current_connection.get() is not None:
        peer_id, message_id = get_marked_id(msg.peer_id), msg.id
        source = transcript_identity(msg, peer_id, cfg.engine, cl)

    data, error = await _download_audio(cl, msg, cfg.max_bytes, cfg)
    if error is not None:
        return error

    try:
        if current_connection.get() is not None:
            await _revalidate_audio_source(
                cl,
                entity if entity is not None else msg.peer_id,
                peer_id,
                message_id,
                cfg.engine,
                source,
            )
        filename, mime = _upload_name(msg)
        return await _post_audio(data, cfg, filename, mime)
    finally:
        data.close()


async def _post_audio(data, cfg, filename, mime):
    data.seek(0, os.SEEK_END)
    size = data.tell()
    data.seek(0)
    if cfg.max_bytes is not None and size > cfg.max_bytes:
        return _too_large(size, cfg.max_bytes, cfg)
    form = {"model": cfg.model, "response_format": cfg.response_format}
    language = transcribe_language()
    if language:
        form["language"] = language
    headers = {"Authorization": f"Bearer {cfg.api_key}"} if cfg.api_key else {}
    try:
        async with asyncio.timeout(cfg.timeout), httpx.AsyncClient(timeout=cfg.timeout) as client:
            resp = await client.post(
                cfg.url,
                headers=headers,
                data=form,
                files={"file": (filename, data, mime)},
            )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("text", ""), str):
            raise ValueError("Invalid transcription response")
        text = (payload.get("text") or "").strip()
    except httpx.HTTPStatusError as error:
        status = error.response.status_code
        if status in (401, 403):
            reason = "provider_credentials"
        elif status == 429 or status >= 500:
            reason = "provider_unavailable"
        elif status == 413:
            reason = "too_large"
        else:
            reason = "provider_rejected"
        return {
            "status": "error",
            "reason": reason,
            "error": f"{cfg.engine} returned HTTP {status}.",
        }
    except httpx.ConnectError:
        return {
            "status": "error",
            "reason": "provider_unavailable",
            "error": "ASR provider connection failed.",
        }
    except AccessDenied:
        raise
    except Exception:
        return {
            "status": "error",
            "reason": "provider_outcome_unknown",
            "error": "ASR request outcome is unknown. Do not retry automatically.",
        }
    if not text:
        return {"status": "error", "error": f"empty transcript from {cfg.engine}"}
    return {"status": "ok", "text": text, "lang": payload.get("language") or language}


async def _transcribe_via_groq(cl, msg) -> dict:
    """Groq whisper-large-v3-turbo."""
    if not os.getenv("GROQ_API_KEY"):
        return {"status": "error", "error": "GROQ_API_KEY is not configured"}
    return await _transcribe_via_openai_compatible(cl, msg, http_engine_config("groq"))


# Local faster-whisper. Loading a model takes seconds and hundreds of MB, so
# one instance per (model, device, compute type) lives for the process. The
# semaphore runs one inference at a time: two concurrent CPU transcriptions
# only slow each other down.
_WHISPER_MODELS: dict = {}
_WHISPER_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _whisper_settings() -> tuple:
    return (
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_MODEL", "").strip() or WHISPER_DEFAULT_MODEL,
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_DEVICE", "").strip() or "auto",
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_COMPUTE_TYPE", "").strip() or "default",
        os.getenv("TELEGRAM_TRANSCRIBE_WHISPER_MODEL_DIR", "").strip() or None,
    )


def _load_whisper_model(settings: tuple):
    model = _WHISPER_MODELS.get(settings)
    if model is None:
        from faster_whisper import WhisperModel

        name, device, compute_type, download_root = settings
        model = WhisperModel(
            name, device=device, compute_type=compute_type, download_root=download_root
        )
        _WHISPER_MODELS[settings] = model
    return model


def _run_whisper(data, settings: tuple, language: Optional[str]) -> tuple:
    model = _load_whisper_model(settings)
    segments, info = model.transcribe(data, language=language, vad_filter=True)
    # segments is a lazy generator: the actual decoding happens here, so it
    # must be consumed inside the worker thread.
    text = "".join(segment.text for segment in segments).strip()
    return text, getattr(info, "language", None)


async def _transcribe_via_whisper(cl, msg) -> dict:
    """Local faster-whisper. The audio never leaves this machine."""
    global _WHISPER_SEMAPHORE
    error = engine_config_error("whisper")
    if error:
        return {"status": "error", "error": error}

    data, error = await _download_audio(cl, msg, None)
    if error is not None:
        return error

    if _WHISPER_SEMAPHORE is None:
        _WHISPER_SEMAPHORE = asyncio.Semaphore(1)
    language = transcribe_language()
    try:
        async with _WHISPER_SEMAPHORE:
            text, lang = await asyncio.to_thread(_run_whisper, data, _whisper_settings(), language)
    except Exception:
        return {"status": "error", "error": "Local whisper transcription failed."}
    finally:
        data.close()

    if not text:
        return {"status": "error", "error": "empty transcript from whisper"}
    return {"status": "ok", "text": text, "lang": lang or language}


async def transcribe(cl, entity, msg, engine: str) -> dict:
    """Dispatch to one engine. Returns a status dict:
    {"status": "ok", "text": ..., "lang": ...} |
    {"status": "pending"} | {"status": "premium_required"} |
    {"status": "error", "error": ...}
    """
    if current_connection.get() is not None:
        from telegram_mcp import transcription_jobs as jobs

        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
        if engine != "openai":
            raise AccessDenied("Dot ASR uses official OpenAI; engine override is not permitted")
        error = engine_config_error(engine)
        if error:
            return {"status": "error", "reason": "provider_configuration", "error": error}
        jobs.recover()
        cfg = http_engine_config(engine)
        peer_id = get_marked_id(msg.peer_id)
        identity = transcript_identity(msg, peer_id, engine, cl)
        duration = voice_duration(msg)
        declared = getattr(getattr(msg, "file", None), "size", None)
        extension = _upload_name(msg)[0].rsplit(".", 1)[-1].lower()
        if (
            jobs.existing(identity)
            or duration is None
            or duration > 600
            or (isinstance(declared, int) and declared > cfg.max_bytes)
            or extension not in {"flac", "mp3", "mp4", "mpeg", "mpga", "m4a", "ogg", "wav", "webm"}
        ):
            result = await jobs.request(cl, entity, msg, cfg, identity)
        elif jobs.busy() or _AUDIO_LOCK.locked():
            result = {"status": "error", "reason": "asr_busy", "complete": False}
        else:
            async with _AUDIO_LOCK:
                result = await _transcribe_via_openai_compatible(cl, msg, cfg, entity=entity)
            if result.get("reason") == "too_large":
                result = await jobs.request(cl, entity, msg, cfg, identity)
        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
        if result.get("reason") != "provider_unavailable":
            return dict(
                result,
                source="openai",
                complete=result.get("complete", result.get("status") == "ok"),
                fallback_reason=None,
            )
        if getattr(msg, "voice", None) is None and getattr(msg, "video_note", None) is None:
            return dict(
                result,
                reason="native_unsupported_media",
                fallback_reason="provider_unavailable",
                complete=False,
            )
        fallback = await _transcribe_via_telegram(cl, entity, msg)
        check_transcript_access(get_marked_id(msg.peer_id), operation=True)
        return dict(
            fallback,
            source="telegram",
            fallback_reason="provider_unavailable",
            complete=fallback.get("status") == "ok",
        )
    if engine == "telegram":
        return await _transcribe_via_telegram(cl, entity, msg)
    if engine == "groq":
        return await _transcribe_via_groq(cl, msg)
    if engine == "whisper":
        return await _transcribe_via_whisper(cl, msg)
    cfg = http_engine_config(engine)
    if cfg is not None:
        return await _transcribe_via_openai_compatible(cl, msg, cfg)
    return {"status": "error", "error": f"Unknown engine '{engine}'"}


# ---------------------------------------------------------------------------
# One paid call per recording
# ---------------------------------------------------------------------------
#
# transcribe_voice and the auto-mode prefetch both do "miss the cache,
# transcribe, save". Two concurrent requests for the same uncached message
# would both miss, both download the audio and both pay Groq for it. The lock
# is keyed by (chat, message, engine) and the cache is re-read inside it, so
# the second caller returns the first one's transcript without a second call.
#
# Scope is one process: the server is a single asyncio process over one SQLite
# cache. Running it as several worker processes would need an atomic claim in
# the database instead - the lock below would not see the other workers.

_INFLIGHT_LOCKS: dict = {}
_AUDIO_LOCK = asyncio.Lock()


@asynccontextmanager
async def _transcribe_lock(key):
    entry = _INFLIGHT_LOCKS.get(key)
    if entry is None:
        entry = [asyncio.Lock(), 0]
        _INFLIGHT_LOCKS[key] = entry
    entry[1] += 1
    try:
        async with entry[0]:
            yield
    finally:
        entry[1] -= 1
        # Dropping the entry keeps the registry from growing with every
        # message ever transcribed, and keeps a lock from outliving the loop
        # it was first awaited on.
        if entry[1] <= 0:
            _INFLIGHT_LOCKS.pop(key, None)


def _cached_result(row: dict) -> dict:
    return {
        "status": "ok",
        "cached": True,
        "text": row["text"],
        "lang": row.get("lang"),
        "duration": row.get("duration"),
        "source": row.get("source"),
        "complete": True,
        "fallback_reason": None,
    }


async def transcribe_cached(
    cl, entity, msg, engine: str, chat_id: int, duration: Optional[int] = None, *, cancel=False
) -> dict:
    """Cache-first transcription, at most one paid call per recording.

    Same status dict as :func:`transcribe`, plus ``cached``. Every caller that
    would otherwise write to the transcript cache should go through here.
    """
    message_id = getattr(msg, "id", None)
    check_transcript_access(chat_id, operation=True)
    if current_connection.get() is not None and engine != "openai":
        raise AccessDenied("Dot ASR uses official OpenAI; engine override is not permitted")
    peer = getattr(msg, "peer_id", None)
    if peer is not None and get_marked_id(peer) != chat_id:
        raise AccessDenied("The requested transcription source changed")
    if current_connection.get() is not None and (
        peer is None or getattr(getattr(msg, "document", None), "id", None) is None
    ):
        raise AccessDenied("The audio has no stable source identity")
    identity = transcript_identity(msg, chat_id, engine, cl)

    await _revalidate_audio_source(cl, entity, chat_id, message_id, engine, identity)
    if cancel:
        if current_connection.get() is None:
            return {"status": "error", "reason": "no_active_job", "complete": False}
        from telegram_mcp import transcription_jobs as jobs

        return await jobs.request(
            cl, entity, msg, http_engine_config(engine), identity, cancel=True
        )
    hit = get_cached_transcript(chat_id, message_id, source=engine, identity=identity)
    if hit is not None:
        return _cached_result(hit)

    async with _transcribe_lock((chat_id, message_id, engine, identity)):
        # Re-read: a concurrent caller may have paid for this one while we
        # were waiting for the lock.
        check_transcript_access(chat_id, operation=True)
        await _revalidate_audio_source(cl, entity, chat_id, message_id, engine, identity)
        hit = get_cached_transcript(chat_id, message_id, source=engine, identity=identity)
        if hit is not None:
            return _cached_result(hit)

        if duration is None:
            duration = voice_duration(msg)
        result = await transcribe(cl, entity, msg, engine)
        check_transcript_access(chat_id, operation=True)
        await _revalidate_audio_source(cl, entity, chat_id, message_id, engine, identity)
        if (
            result.get("status") == "ok"
            and result.get("complete", True)
            and not result.get("job_id")
            and result.get("source", engine) == engine
        ):
            save_transcript(
                chat_id,
                message_id,
                engine,
                result["text"],
                duration=duration,
                lang=result.get("lang"),
                identity=identity,
            )
        out = dict(result)
        out.setdefault("cached", False)
        out.setdefault("duration", duration)
        out.setdefault("source", engine)
        out.setdefault("complete", result.get("status") == "ok")
        out.setdefault("fallback_reason", None)
        return out


# ---------------------------------------------------------------------------
# Batch budget for auto-mixing (mode="auto" only)
# ---------------------------------------------------------------------------
#
# Groq is not a free call like native transcription: each voice message costs
# a download + upload (and a self-hosted engine costs CPU time). Without a
# budget, get_history(limit=100) on a chat with a few dozen voice messages
# would turn into a few dozen downloads/uploads on every read.
# See TELEGRAM_TRANSCRIBE_MAX_VOICES / _MAX_SECONDS.


class TranscribeBudget:
    def __init__(
        self,
        max_voices: Optional[int] = None,
        max_seconds: Optional[int] = None,
    ):
        self.max_voices = max_voices if max_voices is not None else _max_voices_env()
        self.max_seconds = max_seconds if max_seconds is not None else _max_seconds_env()
        self.used_voices = 0
        self.used_seconds = 0

    def can_afford(self, duration: Optional[int]) -> bool:
        if self.used_voices >= self.max_voices:
            return False
        if self.used_seconds + (duration or 0) > self.max_seconds:
            return False
        return True

    def charge(self, duration: Optional[int]) -> None:
        self.used_voices += 1
        self.used_seconds += duration or 0


def _max_voices_env() -> int:
    try:
        return int(os.getenv("TELEGRAM_TRANSCRIBE_MAX_VOICES", "5"))
    except ValueError:
        return 5


def _max_seconds_env() -> int:
    try:
        return int(os.getenv("TELEGRAM_TRANSCRIBE_MAX_SECONDS", "300"))
    except ValueError:
        return 300


async def prefetch_transcripts(cl, entity, chat_id: int, messages) -> None:
    """Fill the cache for cache-miss voice/video-note messages, within budget.

    Only called for TELEGRAM_TRANSCRIBE=auto. on-demand mode leaves cache
    misses to render as "transcript pending" - callers must invoke
    transcribe_voice explicitly to pay for them.
    """
    if current_connection.get() is not None or transcribe_mode() != "auto":
        return
    budget = TranscribeBudget()
    engine = default_engine()
    for msg in messages:
        if not is_transcribable(msg):
            continue
        if getattr(msg, "message", None):
            continue  # already has text, nothing to fill
        if (
            get_cached_transcript(
                chat_id,
                msg.id,
                source=engine,
                identity=transcript_identity(msg, chat_id, engine, cl),
            )
            is not None
        ):
            continue
        duration = voice_duration(msg)
        if not budget.can_afford(duration):
            continue
        budget.charge(duration)
        try:
            # Shares the lock with transcribe_voice: a prefetch and an explicit
            # call for the same recording pay for it once between them.
            await transcribe_cached(cl, entity, msg, engine, chat_id, duration=duration)
        except Exception:
            continue


# ---------------------------------------------------------------------------
# Render helper shared by message_to_dict / format_message_line / list_messages
# ---------------------------------------------------------------------------


def voice_attachment_info(msg, chat_id: Optional[int]) -> Optional[dict]:
    """None for non-voice-like messages. Otherwise:
    {"duration": int|None,
     "transcript": str|None, "transcript_source": str|None,
     "transcript_status": "ready"|"pending"|None}

    transcript_status is None when transcription is off or no chat_id is
    available (duration still fills in - it's free, already on the fetched
    message). "pending" covers both "never attempted" (on-demand mode,
    budget exhausted) and "attempted but Telegram is still processing" -
    callers don't need to tell those apart, both mean "call transcribe_voice
    later".
    """
    if not is_transcribable(msg):
        return None
    info = {
        "duration": voice_duration(msg),
        "transcript": None,
        "transcript_source": None,
        "transcript_status": None,
    }
    if chat_id is None or transcribe_mode() == "off":
        return info
    try:
        check_transcript_access(chat_id)
    except AccessDenied:
        return info
    engines = (
        ["openai"]
        if current_connection.get() is not None
        else [default_engine()] + sorted(ENGINES - {default_engine()})
    )
    cached = None
    for engine in engines:
        cached = get_cached_transcript(
            chat_id, msg.id, source=engine, identity=transcript_identity(msg, chat_id, engine)
        )
        if cached is not None:
            break
    if cached is not None:
        info["transcript"] = cached["text"]
        info["transcript_source"] = cached["source"]
        info["transcript_status"] = "ready"
    else:
        info["transcript_status"] = "pending"
    return info


def render_voice_text(info: dict) -> str:
    """`voice 0:23` / `voice 0:23 | transcript: ... (source: groq)` / with a
    pending marker - never presented as a verbatim quote."""
    label = f"voice {format_duration(info['duration'])}"
    status = info["transcript_status"]
    if status == "ready":
        return (
            f"{label} | transcript ({info['transcript_source']}, not verbatim): "
            f"{info['transcript']}"
        )
    if status == "pending":
        return f"{label} | transcript pending"
    return label
