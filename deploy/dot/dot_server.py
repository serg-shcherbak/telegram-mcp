from __future__ import annotations

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from telegram_mcp import access
from telegram_mcp.access import DotMCP, load_access_config, validate_media_root


def prepare():
    load_dotenv()
    raw_path = os.getenv("DOT_ACCESS_CONFIG")
    if not raw_path or not Path(raw_path).is_absolute():
        raise ValueError("DOT_ACCESS_CONFIG must name a private absolute configuration path")
    config_path = Path(raw_path)
    config = load_access_config(config_path)
    for key, safe in {
        "MCP_TRANSPORT": "http",
        "TELEGRAM_SESSION_LOCK": "exclusive",
        "TELEGRAM_LEGACY_EVENTS": "0",
        "TELEGRAM_EVENT_FEED": "0",
        "TELEGRAM_SERVER_ROOTS_ONLY": "1",
    }.items():
        if os.environ.setdefault(key, safe).strip().lower() != safe:
            raise ValueError(f"Unsafe setting for {key}")
    if os.environ.setdefault("TELEGRAM_TRANSCRIBE", "on-demand") not in ("on-demand", "off"):
        raise ValueError("Dot transcription must be on-demand or off")
    if any(
        key.startswith("TELEGRAM_SESSION_STRING") or key.startswith("TELEGRAM_SESSION_NAME_")
        for key, value in os.environ.items()
        if value
    ):
        raise ValueError("Dot requires one existing own file session")
    session_name = os.getenv("TELEGRAM_SESSION_NAME")
    if not session_name or not Path(session_name).is_absolute():
        raise ValueError("An existing absolute Telegram file session is required")
    session = Path(session_name)
    if session.suffix != ".session":
        session = Path(str(session) + ".session")
    if not session.is_file() or session.is_symlink():
        raise ValueError("An existing own file session is required")
    info = session.stat()
    if info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("Telegram file session must be owner-only")
    source = Path(access.__file__).resolve().parent.parent
    if source in session.resolve().parents or source in config_path.resolve().parents:
        raise ValueError("Session and access configuration must be outside the checkout")
    validate_media_root(config.media_root, config_path, session)
    entrypoint_root = Path(__file__).resolve().parent
    if (
        config.media_root == entrypoint_root
        or entrypoint_root in config.media_root.parents
        or config.media_root in entrypoint_root.parents
    ):
        raise ValueError("Media root overlaps the server entrypoint directory")
    config.media_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.environ["TELEGRAM_ALLOWED_ROOTS"] = str(config.media_root)

    from telegram_mcp import runner, runtime
    from telegram_mcp.tools import events

    if len(runtime.clients) != 1 or set(runtime.clients) != {"default"}:
        raise ValueError("Dot requires exactly one owner account")
    if events.feed_enabled():
        raise ValueError("Stop existing observation before starting Dot")
    events.register_incoming_handlers()
    server = DotMCP(config_path, runtime.mcp)
    if server._dot_owner_id != config.owner_id or server._dot_media_root != config.media_root:
        raise ValueError("Owner settings changed during startup; restart required")
    runtime.SERVER_ALLOWED_ROOTS = [config.media_root]
    runner.mcp = server
    return runner


def main():
    runner = prepare()
    asyncio.run(runner._main())


if __name__ == "__main__":
    main()
