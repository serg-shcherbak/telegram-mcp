#!/usr/bin/env python3
"""Migrate a Telegram StringSession to a persistent SQLite session."""

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import SQLiteSession, StringSession

from telegram_mcp.account_labels import canonical_account_label
from telegram_mcp.client_identity import client_identity_kwargs
from telegram_mcp.install_guard import UnsafeInstallationError, assert_safe_distribution
from telegram_mcp.private_session import (
    create_private_session,
    default_session_target,
    remove_created_session,
    session_paths,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Migrate TELEGRAM_SESSION_STRING to a persistent SQLite session. "
            "Stop any running telegram-mcp process before migrating."
        )
    )
    parser.add_argument(
        "--account",
        metavar="LABEL",
        help=(
            "Migrate TELEGRAM_SESSION_STRING_<LABEL> instead of the default account "
            "(for example: --account work)."
        ),
    )
    parser.add_argument(
        "--target",
        metavar="PATH",
        help=(
            "Destination session name or path, without the .session suffix. "
            "Defaults to TELEGRAM_SESSION_NAME[_<LABEL>] or a private XDG state directory."
        ),
    )
    return parser.parse_args()


def _check_installation() -> None:
    try:
        assert_safe_distribution()
    except UnsafeInstallationError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


def _env_names(account: Optional[str]) -> tuple[str, str, str]:
    if not account:
        return (
            "TELEGRAM_SESSION_STRING",
            "TELEGRAM_SESSION_NAME",
            "telegram_mcp_session",
        )

    label = account.strip()
    if not label or not label.replace("-", "").replace("_", "").isalnum():
        raise ValueError("account label may contain only letters, numbers, '-' and '_'")

    canonical = canonical_account_label(label)
    return (
        f"TELEGRAM_SESSION_STRING_{canonical.upper()}",
        f"TELEGRAM_SESSION_NAME_{canonical.upper()}",
        f"telegram_mcp_session_{canonical}",
    )


def _target_paths(target: str) -> tuple[str, Path]:
    """Return the Telethon session name and its resulting SQLite filename."""
    return session_paths(target)


async def migrate(
    session_string: str,
    api_id: int,
    api_hash: str,
    target: str,
) -> tuple[Path, object]:
    """Copy auth material into a new SQLite session and verify it with Telegram."""
    session_name, session_file = _target_paths(target)
    identity = create_private_session(session_file)
    sqlite_session = None
    client = None
    completed = False
    try:
        source = StringSession(session_string)
        sqlite_session = SQLiteSession(session_name)
        sqlite_session.set_dc(source.dc_id, source.server_address, source.port)
        sqlite_session.auth_key = source.auth_key
        sqlite_session.save()

        client = TelegramClient(
            sqlite_session,
            api_id,
            api_hash,
            **client_identity_kwargs(),
        )
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("the source session is not authorized")
        user = await client.get_me()
        await client.disconnect()
        client = None
        completed = True
    finally:
        try:
            if client is not None:
                await client.disconnect()
        finally:
            try:
                if sqlite_session is not None:
                    sqlite_session.close()
            finally:
                if not completed:
                    remove_created_session(session_file, identity)
    return session_file, user


async def _run(args: argparse.Namespace) -> int:
    try:
        string_env, name_env, default_target = _env_names(args.account)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    api_id_raw = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_string = os.getenv(string_env)

    if not api_id_raw or not api_hash:
        print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set", file=sys.stderr)
        return 1
    try:
        api_id = int(api_id_raw)
    except ValueError:
        print("Error: TELEGRAM_API_ID must be an integer", file=sys.stderr)
        return 1
    if not session_string:
        print(f"Error: {string_env} must be set", file=sys.stderr)
        return 1

    try:
        target = args.target or os.getenv(name_env) or default_session_target(default_target)
        session_file, _user = await migrate(session_string, api_id, api_hash, target)
    except Exception as exc:
        print(f"Migration failed ({type(exc).__name__}); source unchanged", file=sys.stderr)
        return 1

    session_name = str(session_file.with_suffix(""))
    print("Migration successful")
    print(f"Created {session_file}")
    print("\nUpdate your environment:")
    print(f"  {name_env}={session_name}")
    print(f"  remove or comment out {string_env}")
    print("\nKeep the .session file private and do not commit it.")
    return 0


def main() -> None:
    load_dotenv()
    _check_installation()
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
