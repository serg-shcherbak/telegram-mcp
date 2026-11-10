import asyncio
import os
from types import SimpleNamespace

import pytest
from telethon.crypto import AuthKey

import migrate_session


def test_env_names_support_default_and_labeled_accounts():
    assert migrate_session._env_names(None) == (
        "TELEGRAM_SESSION_STRING",
        "TELEGRAM_SESSION_NAME",
        "telegram_mcp_session",
    )
    assert migrate_session._env_names("Work-2") == (
        "TELEGRAM_SESSION_STRING_WORK_2",
        "TELEGRAM_SESSION_NAME_WORK_2",
        "telegram_mcp_session_work_2",
    )


def test_env_names_reject_invalid_account_label():
    with pytest.raises(ValueError, match="account label"):
        migrate_session._env_names("work account")


def test_target_paths_accepts_optional_session_suffix(tmp_path):
    without_suffix = tmp_path / "telegram"
    with_suffix = tmp_path / "telegram.session"

    assert migrate_session._target_paths(str(without_suffix)) == (
        str(without_suffix),
        with_suffix.resolve(),
    )
    assert migrate_session._target_paths(str(with_suffix)) == (
        str(without_suffix),
        with_suffix.resolve(),
    )


@pytest.mark.asyncio
async def test_migrate_copies_auth_key_verifies_and_hardens_file(tmp_path, monkeypatch):
    target = tmp_path / "telegram"
    session_file = tmp_path / "telegram.session"
    source = SimpleNamespace(
        dc_id=2,
        server_address="149.154.167.40",
        port=443,
        auth_key=object(),
    )
    saved = {}

    class FakeSQLiteSession:
        def __init__(self, name):
            saved["name"] = name
            assert session_file.stat().st_mode & 0o777 == 0o600
            assert session_file.parent.stat().st_mode & 0o777 == 0o700
            session_file.touch()

        def set_dc(self, dc_id, address, port):
            saved["dc"] = (dc_id, address, port)

        def save(self):
            saved["saved"] = True

        def close(self):
            saved["closed"] = True

        @property
        def auth_key(self):
            return saved.get("auth_key")

        @auth_key.setter
        def auth_key(self, value):
            saved["auth_key"] = value

    class FakeClient:
        def __init__(self, session, api_id, api_hash, **kwargs):
            saved["client"] = (session, api_id, api_hash, kwargs)

        async def connect(self):
            saved["connected"] = True

        async def is_user_authorized(self):
            return True

        async def get_me(self):
            return SimpleNamespace(id=42, first_name="Ada")

        async def disconnect(self):
            saved["disconnected"] = True

    monkeypatch.setattr(migrate_session, "StringSession", lambda value: source)
    monkeypatch.setattr(migrate_session, "SQLiteSession", FakeSQLiteSession)
    monkeypatch.setattr(migrate_session, "TelegramClient", FakeClient)
    monkeypatch.setattr(
        migrate_session, "client_identity_kwargs", lambda: {"device_model": "test"}
    )

    created, user = await migrate_session.migrate("secret", 123, "hash", str(target))

    assert created == session_file.resolve()
    assert user.id == 42
    assert saved["dc"] == (2, "149.154.167.40", 443)
    assert saved["auth_key"] is source.auth_key
    assert saved["saved"] is True
    assert saved["client"][1:] == (123, "hash", {"device_model": "test"})
    assert saved["connected"] is True
    assert saved["disconnected"] is True
    if os.name != "nt":  # POSIX permissions only
        assert session_file.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_migrate_refuses_to_overwrite_existing_session(tmp_path):
    session_file = tmp_path / "telegram.session"
    session_file.touch()

    with pytest.raises(FileExistsError, match="already exists"):
        await migrate_session.migrate("secret", 123, "hash", str(tmp_path / "telegram"))


@pytest.mark.asyncio
async def test_migrate_removes_new_file_when_verification_fails(tmp_path, monkeypatch):
    target = tmp_path / "telegram"
    session_file = tmp_path / "telegram.session"
    source = SimpleNamespace(dc_id=2, server_address="server", port=443, auth_key=object())

    class FakeSQLiteSession:
        def __init__(self, name):
            session_file.touch()

        def set_dc(self, *args):
            return None

        def save(self):
            return None

        def close(self):
            return None

        auth_key = None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            return None

        async def connect(self):
            return None

        async def is_user_authorized(self):
            return False

        async def disconnect(self):
            return None

    monkeypatch.setattr(migrate_session, "StringSession", lambda value: source)
    monkeypatch.setattr(migrate_session, "SQLiteSession", FakeSQLiteSession)
    monkeypatch.setattr(migrate_session, "TelegramClient", FakeClient)

    with pytest.raises(RuntimeError, match="not authorized"):
        await migrate_session.migrate("secret", 123, "hash", str(target))

    assert not session_file.exists()


@pytest.mark.asyncio
async def test_run_reports_missing_source_session(monkeypatch, capsys):
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.delenv("TELEGRAM_SESSION_STRING", raising=False)

    result = await migrate_session._run(SimpleNamespace(account=None, target=None))

    assert result == 1
    assert "TELEGRAM_SESSION_STRING must be set" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_migrate_cancelled_verification_cleans_target_and_preserves_source(
    tmp_path, monkeypatch
):
    source = "synthetic-source-session"
    monkeypatch.setenv("TELEGRAM_SESSION_STRING", source)
    monkeypatch.setattr(
        migrate_session,
        "StringSession",
        lambda value: SimpleNamespace(
            dc_id=2, server_address="server", port=443, auth_key=AuthKey(data=b"x" * 256)
        ),
    )

    class CancelledClient:
        def __init__(self, *args, **kwargs):
            pass

        async def connect(self):
            raise asyncio.CancelledError

        async def disconnect(self):
            pass

    monkeypatch.setattr(migrate_session, "TelegramClient", CancelledClient)
    target = tmp_path / "telegram"
    with pytest.raises(asyncio.CancelledError):
        await migrate_session.migrate(source, 123, "hash", str(target))
    assert not target.with_suffix(".session").exists()
    assert os.environ["TELEGRAM_SESSION_STRING"] == source


@pytest.mark.asyncio
async def test_migrate_refuses_dangling_symlink_and_preserves_it(tmp_path):
    session = tmp_path / "telegram.session"
    missing = tmp_path / "missing"
    session.symlink_to(missing)
    with pytest.raises(FileExistsError, match="already exists"):
        await migrate_session.migrate("secret", 123, "hash", str(session))
    assert session.is_symlink()
    assert not missing.exists()


@pytest.mark.asyncio
async def test_migrate_refuses_checkout_and_media_targets(monkeypatch, tmp_path):
    from pathlib import Path

    monkeypatch.setenv("TELEGRAM_ALLOWED_ROOTS", str(tmp_path / "media"))
    for target in (
        Path(migrate_session.__file__).parent / "private-test",
        tmp_path / "media" / "auth",
    ):
        with pytest.raises(ValueError, match="outside"):
            await migrate_session.migrate("secret", 123, "hash", str(target))
        assert not target.with_suffix(".session").exists()


@pytest.mark.asyncio
async def test_run_default_uses_private_state_path_without_printing_source(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setenv("TELEGRAM_SESSION_STRING", "synthetic-source-session")
    monkeypatch.delenv("TELEGRAM_SESSION_NAME", raising=False)
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path))
    targets = []

    async def fake_migrate(source, api_id, api_hash, target):
        targets.append(target)
        return tmp_path / "telegram.session", SimpleNamespace(id=42)

    monkeypatch.setattr(migrate_session, "migrate", fake_migrate)
    assert await migrate_session._run(SimpleNamespace(account=None, target=None)) == 0
    assert targets == [str(tmp_path / "telegram-mcp" / "sessions" / "telegram_mcp_session")]
    assert "synthetic-source-session" not in capsys.readouterr().out


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["-journal", "-wal", "-shm"])
async def test_migrate_preserves_preexisting_sqlite_sidecars(tmp_path, suffix):
    session = tmp_path / "telegram.session"
    sidecar = tmp_path / f"telegram.session{suffix}"
    sidecar.write_bytes(b"existing-sidecar")
    with pytest.raises(FileExistsError, match="already exists"):
        await migrate_session.migrate("secret", 123, "hash", str(session))
    assert sidecar.read_bytes() == b"existing-sidecar"
    assert not session.exists()


@pytest.mark.asyncio
async def test_migrate_rejects_permissive_parent_before_writing(tmp_path):
    directory = tmp_path / "public"
    directory.mkdir(mode=0o755)
    with pytest.raises(ValueError, match="owner-only"):
        await migrate_session.migrate("secret", 123, "hash", str(directory / "telegram"))
    assert list(directory.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("separator", [",", ";", os.pathsep])
async def test_migrate_checks_every_configured_media_root(monkeypatch, tmp_path, separator):
    media = tmp_path / "media"
    monkeypatch.setenv("TELEGRAM_ALLOWED_ROOTS", f"{tmp_path / 'other'}{separator}{media}")
    with pytest.raises(ValueError, match="outside"):
        await migrate_session.migrate("secret", 123, "hash", str(media / "telegram"))
    assert not media.exists()


@pytest.mark.asyncio
async def test_migrate_checks_dot_media_root_without_loading_client(monkeypatch, tmp_path):
    import json

    media = tmp_path / "media"
    config = tmp_path / "access.json"
    config.write_text(json.dumps({"media_root": str(media)}), encoding="utf-8")
    monkeypatch.setenv("DOT_ACCESS_CONFIG", str(config))
    with pytest.raises(ValueError, match="outside"):
        await migrate_session.migrate("secret", 123, "hash", str(media / "telegram"))
    assert not media.exists()


@pytest.mark.asyncio
async def test_migration_error_does_not_echo_source(monkeypatch, capsys):
    source = "synthetic-source-session"
    monkeypatch.setenv("TELEGRAM_SESSION_STRING", source)

    async def rejected(*args):
        raise ValueError(source)

    monkeypatch.setattr(migrate_session, "migrate", rejected)
    assert await migrate_session._run(SimpleNamespace(account=None, target=None)) == 1
    assert source not in capsys.readouterr().err


def test_failed_session_cleanup_preserves_replaced_file_and_symlink(tmp_path):
    from telegram_mcp.private_session import create_private_session, remove_created_session

    session = tmp_path / "telegram.session"
    identity = create_private_session(session)
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"keep")
    replacement.replace(session)
    remove_created_session(session, identity)
    assert session.read_bytes() == b"keep"
    session.unlink()
    session.symlink_to(replacement)
    remove_created_session(session, identity)
    assert session.is_symlink()


def test_private_file_setup_fails_before_writes_on_native_windows(monkeypatch, tmp_path):
    from telegram_mcp import private_session

    target = tmp_path / "new" / "telegram.session"
    with monkeypatch.context() as platform:
        platform.setattr(private_session.os, "name", "nt")
        with pytest.raises(ValueError, match="Windows ACLs are unverified"):
            private_session.create_private_session(target)
    assert not target.parent.exists()
