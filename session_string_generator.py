#!/usr/bin/env python3
"""Create a private native Telegram session for the MCP server.

Usage: python session_string_generator.py [--qr | --phone]
Stop the server before onboarding. Reuse an existing own session instead of
running this command when the account is already connected.
"""

import argparse
import asyncio
import getpass
import io
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from telethon import errors
from telethon.sessions import SQLiteSession
from telethon.sync import TelegramClient
from telegram_mcp.account_labels import canonical_account_label
from telegram_mcp.client_identity import client_identity_kwargs
from telegram_mcp.install_guard import UnsafeInstallationError, assert_safe_distribution
from telegram_mcp.private_session import (
    create_private_session,
    default_session_target,
    remove_created_session,
    session_paths,
)

# How many times the QR code is regenerated after expiry before giving up.
_QR_MAX_REFRESHES = 10


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a private native Telegram session for telegram-mcp."
    )
    login_group = parser.add_mutually_exclusive_group()
    login_group.add_argument(
        "--qr",
        action="store_true",
        help="Use Telegram QR login without prompting for a login method.",
    )
    login_group.add_argument(
        "--phone",
        action="store_true",
        help="Use phone number + verification code login without prompting for a login method.",
    )
    return parser.parse_args()


def _check_installation() -> None:
    try:
        assert_safe_distribution()
    except UnsafeInstallationError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


def _stream_can_encode(text: str, stream) -> bool:
    """True if *stream* can write *text* without raising UnicodeEncodeError."""
    encoding = getattr(stream, "encoding", None)
    if not encoding:
        return True
    try:
        text.encode(encoding)
    except (LookupError, UnicodeEncodeError):
        return False
    return True


def _harden_stdio() -> None:
    """Replace unencodable output instead of crashing mid-login.

    On Windows, redirected stdout falls back to the legacy ANSI code page
    (e.g. cp1252), which cannot encode the QR block characters or non-latin
    text, so a bare print() raises UnicodeEncodeError and aborts the login.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass


def _render_qr(qr) -> None:
    import qrcode

    print("\n----- QR Code Login -----\n")

    qr_obj = qrcode.QRCode(border=1)
    qr_obj.add_data(qr.url)
    qr_obj.make(fit=True)
    f = io.StringIO()
    qr_obj.print_ascii(out=f, invert=True)
    art = f.getvalue()

    if _stream_can_encode(art, sys.stdout):
        print(art)
        print("Scan the QR code above with your Telegram app:")
        print("  Open Telegram > Settings > Devices > Link Desktop Device\n")
        print(f"Or open this link on a device where you're logged in:\n  {qr.url}\n")
    else:
        encoding = getattr(sys.stdout, "encoding", None) or "unknown"
        print(f"This console ({encoding}) cannot draw the QR code.")
        print(f"Open this link on a device where you're logged in instead:\n  {qr.url}\n")

    print(f"Expires at: {qr.expires.strftime('%H:%M:%S')}")
    print("Waiting for you to scan...")


def _seconds_until_expiry(qr) -> float:
    """Seconds left before this QR token expires, with a small safety margin."""
    expires = qr.expires
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    remaining = (expires - datetime.now(timezone.utc)).total_seconds()
    return max(1.0, remaining - 1.0)


def _qr_login(client: TelegramClient) -> None:
    qr = client.qr_login()
    _render_qr(qr)

    for _ in range(_QR_MAX_REFRESHES):
        try:
            client.loop.run_until_complete(qr.wait(timeout=_seconds_until_expiry(qr)))
            return
        except asyncio.TimeoutError:
            client.loop.run_until_complete(qr.recreate())
            print("\nQR code expired, here is a fresh one.")
            _render_qr(qr)
        except errors.SessionPasswordNeededError:
            while True:
                pw = getpass.getpass(
                    "\nTwo-factor authentication enabled. Please enter your password: "
                )
                try:
                    client.sign_in(password=pw)
                    return
                except errors.PasswordHashInvalidError:
                    print("Invalid password, please try again.")

    print("\nQR code expired too many times. Please run the generator again.")
    client.disconnect()
    sys.exit(1)


def _phone_login(client: TelegramClient) -> None:
    phone = input("Please enter your phone (or bot token): ")

    try:
        client.send_code_request(phone)
    except errors.FloodWaitError as e:
        print(f"\nFlood wait error; you must wait {e.seconds} seconds before trying again.")
        client.disconnect()
        sys.exit(1)
    except errors.PhoneNumberInvalidError:
        print("\nThe phone number is invalid.")
        client.disconnect()
        sys.exit(1)
    except Exception as e:
        print(f"\nError sending code: {e}")
        client.disconnect()
        sys.exit(1)

    code = input("\nPlease enter the code you received: ")
    try:
        client.sign_in(phone, code)
    except errors.SessionPasswordNeededError:
        pw = getpass.getpass("Two-factor authentication enabled. Please enter your password: ")
        client.sign_in(password=pw)


def main() -> None:
    _harden_stdio()
    load_dotenv()
    args = _parse_args()
    _check_installation()

    API_ID = os.getenv("TELEGRAM_API_ID")
    API_HASH = os.getenv("TELEGRAM_API_HASH")

    if not API_ID or not API_HASH:
        print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env file")
        print("Create an .env file with your credentials from https://my.telegram.org/apps")
        sys.exit(1)

    try:
        API_ID = int(API_ID)
    except ValueError:
        print("Error: TELEGRAM_API_ID must be an integer")
        sys.exit(1)

    print("\n----- Private Telegram Session Setup -----\n")
    print("Stop the server first. Reuse your existing own session if already connected.")

    try:
        label = (
            input("Account label (optional, e.g. 'work', 'personal'; leave empty for default): ")
            .strip()
            .lower()
        )
    except EOFError:
        # Non-interactive stdin (piped/scripted runs): fall back to the default label.
        label = ""
    # The suggested env var must match what the migrator and the server read:
    # `export TELEGRAM_SESSION_STRING_MY-WORK=...` is not valid shell syntax.
    label = canonical_account_label(label)

    if args.qr:
        method = "1"
    elif args.phone:
        method = "2"
    else:
        print("\nChoose login method:")
        print("  1) QR code login (recommended -- scan from your Telegram app)")
        print("  2) Phone number + verification code")
        method = input("\nEnter 1 or 2 [default: 1]: ").strip() or "1"

    name_env = f"TELEGRAM_SESSION_NAME_{label.upper()}" if label else "TELEGRAM_SESSION_NAME"
    default_name = f"telegram_mcp_session_{label}" if label else "telegram_mcp_session"
    session = None
    client = None
    session_file = None
    identity = None
    completed = False
    try:
        target = os.getenv(name_env) or default_session_target(default_name)
        session_name, session_file = session_paths(target)
        identity = create_private_session(session_file)
        session = SQLiteSession(session_name)
        client = TelegramClient(session, API_ID, API_HASH, **client_identity_kwargs())
        client.connect()

        if not client.is_user_authorized():
            if method == "1":
                _qr_login(client)
            else:
                _phone_login(client)
        if not client.is_user_authorized():
            raise RuntimeError("Authentication was not completed")
        session.save()
        client.disconnect()
        client = None
        completed = True
    except Exception as exc:
        print(
            f"Authentication failed ({type(exc).__name__}); no new session retained",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    finally:
        try:
            if client is not None:
                client.disconnect()
        finally:
            try:
                if session is not None:
                    session.close()
            finally:
                if not completed and identity is not None:
                    remove_created_session(session_file, identity)

    print("\nAuthentication successful!")
    print(f"Private session: {session_file}")
    print(f"Set {name_env}={session_name}")
    print("Keep this file private. No environment file was changed.")


if __name__ == "__main__":
    main()
