import asyncio
import io
import sys
from datetime import datetime, timedelta, timezone

import pytest

import session_string_generator


class _NS:
    def __init__(self, expires):
        self.expires = expires


class _FakeQR:
    def __init__(self, wait_outcomes):
        # Each entry is either an exception to raise on wait(), or None to succeed.
        self._wait_outcomes = list(wait_outcomes)
        self.url = "tg://login?token=fake"
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)
        self.recreate_calls = 0

    def wait(self, timeout=None):
        return ("wait", self._wait_outcomes.pop(0))

    def recreate(self):
        self.recreate_calls += 1
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)
        return ("recreate", None)


class _FakeLoop:
    def run_until_complete(self, marker):
        kind, outcome = marker
        if kind == "wait" and outcome is not None:
            raise outcome
        return None


class _FakeClient:
    def __init__(self, qr):
        self._qr = qr
        self.loop = _FakeLoop()
        self.disconnected = False
        self.sign_in_calls = []

    def qr_login(self):
        return self._qr

    def sign_in(self, password=None):
        self.sign_in_calls.append(password)

    def disconnect(self):
        self.disconnected = True


def test_seconds_until_expiry_handles_naive_and_aware():
    naive = datetime(2999, 1, 1)
    aware = datetime.now(timezone.utc) + timedelta(seconds=30)

    assert session_string_generator._seconds_until_expiry(_NS(naive)) > 1.0
    assert session_string_generator._seconds_until_expiry(_NS(aware)) > 1.0


def test_seconds_until_expiry_never_below_one():
    past = datetime.now(timezone.utc) - timedelta(seconds=10)

    assert session_string_generator._seconds_until_expiry(_NS(past)) == 1.0


def test_qr_login_returns_on_first_successful_scan():
    qr = _FakeQR([None])
    client = _FakeClient(qr)

    session_string_generator._qr_login(client)

    assert qr.recreate_calls == 0
    assert client.disconnected is False


def test_qr_login_refreshes_after_expiry_then_succeeds():
    qr = _FakeQR([asyncio.TimeoutError(), None])
    client = _FakeClient(qr)

    session_string_generator._qr_login(client)

    assert qr.recreate_calls == 1
    assert client.disconnected is False


def test_qr_login_gives_up_after_max_refreshes(monkeypatch):
    monkeypatch.setattr(session_string_generator, "_QR_MAX_REFRESHES", 3)
    qr = _FakeQR([asyncio.TimeoutError()] * 3)
    client = _FakeClient(qr)

    with pytest.raises(SystemExit):
        session_string_generator._qr_login(client)

    assert qr.recreate_calls == 3
    assert client.disconnected is True


def test_qr_login_uses_hidden_password_prompt_for_2fa(monkeypatch):
    qr = _FakeQR([session_string_generator.errors.SessionPasswordNeededError(request=None)])
    client = _FakeClient(qr)
    prompted = []

    monkeypatch.setattr("getpass.getpass", lambda prompt: prompted.append(prompt) or "secret")

    session_string_generator._qr_login(client)

    assert prompted == ["\nTwo-factor authentication enabled. Please enter your password: "]
    assert client.sign_in_calls == ["secret"]


def test_qr_login_retries_2fa_on_invalid_password(monkeypatch):
    qr = _FakeQR([session_string_generator.errors.SessionPasswordNeededError(request=None)])
    client = _FakeClient(qr)
    attempts = []

    def sign_in(password=None):
        attempts.append(password)
        if len(attempts) == 1:
            raise session_string_generator.errors.PasswordHashInvalidError(request=None)

    client.sign_in = sign_in
    entered = iter(["wrong", "right"])
    monkeypatch.setattr("getpass.getpass", lambda prompt: next(entered))

    session_string_generator._qr_login(client)

    assert attempts == ["wrong", "right"]


def test_phone_login_uses_hidden_password_prompt_for_2fa(monkeypatch):
    class _Client:
        def __init__(self):
            self.sign_in_calls = []

        def send_code_request(self, phone):
            self.phone = phone

        def disconnect(self):
            return None

        def sign_in(self, *args, **kwargs):
            if kwargs.get("password") is not None:
                self.sign_in_calls.append(kwargs["password"])
                return None
            raise session_string_generator.errors.SessionPasswordNeededError(request=None)

    client = _Client()
    prompted = []
    entered = iter(["+10000000000", "12345"])

    monkeypatch.setattr("builtins.input", lambda _prompt="": next(entered))
    monkeypatch.setattr("getpass.getpass", lambda prompt: prompted.append(prompt) or "secret")

    session_string_generator._phone_login(client)

    assert client.phone == "+10000000000"
    assert prompted == ["Two-factor authentication enabled. Please enter your password: "]
    assert client.sign_in_calls == ["secret"]


def _bytes_stdout(encoding):
    """A strict TextIOWrapper standing in for a redirected console stream."""
    return io.TextIOWrapper(io.BytesIO(), encoding=encoding, errors="strict", write_through=True)


def test_render_qr_prints_ascii_art_when_console_can_encode(monkeypatch):
    qr = _FakeQR([])
    stdout = _bytes_stdout("utf-8")
    monkeypatch.setattr(sys, "stdout", stdout)

    session_string_generator._render_qr(qr)

    out = stdout.buffer.getvalue().decode("utf-8")
    assert "▄" in out  # the half-block characters of the QR art
    assert qr.url in out


def test_render_qr_falls_back_to_link_when_console_cannot_encode(monkeypatch):
    # Redirected stdout on Windows uses the ANSI code page (cp1252), which
    # cannot encode the QR block characters and used to crash the generator.
    qr = _FakeQR([])
    stdout = _bytes_stdout("cp1252")
    monkeypatch.setattr(sys, "stdout", stdout)

    session_string_generator._render_qr(qr)

    out = stdout.buffer.getvalue().decode("cp1252")
    assert "cannot draw the QR code" in out
    assert qr.url in out
    assert "Waiting for you to scan..." in out


def test_harden_stdio_replaces_unencodable_output_instead_of_crashing(monkeypatch):
    stdout = _bytes_stdout("cp1252")
    stderr = _bytes_stdout("cp1252")
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stderr", stderr)

    session_string_generator._harden_stdio()
    print("session for █Ж█ ready")  # raises under strict cp1252

    out = stdout.buffer.getvalue().decode("cp1252")
    assert "session for" in out
    assert "ready" in out


def test_parse_args_qr_selects_qr_login(monkeypatch):
    monkeypatch.setattr("sys.argv", ["session_string_generator.py", "--qr"])

    args = session_string_generator._parse_args()

    assert args.qr is True
    assert args.phone is False


def test_parse_args_phone_selects_phone_login(monkeypatch):
    monkeypatch.setattr("sys.argv", ["session_string_generator.py", "--phone"])

    args = session_string_generator._parse_args()

    assert args.qr is False
    assert args.phone is True


def test_parse_args_without_flags_keeps_interactive_login_choice(monkeypatch):
    monkeypatch.setattr("sys.argv", ["session_string_generator.py"])

    args = session_string_generator._parse_args()

    assert args.qr is False
    assert args.phone is False


def test_parse_args_rejects_conflicting_login_modes(monkeypatch):
    monkeypatch.setattr("sys.argv", ["session_string_generator.py", "--qr", "--phone"])

    with pytest.raises(SystemExit):
        session_string_generator._parse_args()


class _AuthorizedClient:
    def __init__(self, *args, **kwargs):
        self.session = args[0]
        self.disconnected = False

    def connect(self):
        pass

    def is_user_authorized(self):
        return True

    def disconnect(self):
        self.disconnected = True


@pytest.fixture
def private_login(monkeypatch, tmp_path):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.delenv("TELEGRAM_SESSION_NAME", raising=False)
    monkeypatch.delenv("DOT_ACCESS_CONFIG", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_ROOTS", raising=False)
    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setattr("sys.argv", ["session_string_generator.py", "--qr"])
    monkeypatch.setattr(session_string_generator, "_harden_stdio", lambda: None)
    monkeypatch.setattr(session_string_generator, "_check_installation", lambda: None)
    monkeypatch.setattr(session_string_generator, "client_identity_kwargs", dict)
    monkeypatch.setattr(session_string_generator, "TelegramClient", _AuthorizedClient)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "")
    return tmp_path / "state" / "telegram-mcp" / "sessions" / "telegram_mcp_session.session"


def test_login_persists_private_native_session_without_secret_output(
    monkeypatch, tmp_path, capsys, private_login
):
    env_file = tmp_path / ".env"
    original = "# Настройки Telegram\nTELEGRAM_API_ID=1\n"
    env_file.write_text(original, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    writes = []

    class NativeSession:
        def __init__(self, name):
            target = private_login
            writes.append((target.stat().st_mode & 0o777, target.parent.stat().st_mode & 0o777))
            target.write_bytes(b"synthetic-auth-material")

        def save(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(session_string_generator, "SQLiteSession", NativeSession, raising=False)
    session_string_generator.main()

    out = capsys.readouterr().out
    assert "synthetic-auth-material" not in out
    assert f"TELEGRAM_SESSION_NAME={private_login.with_suffix('')}" in out
    assert writes == [(0o600, 0o700)]
    assert private_login.read_bytes() == b"synthetic-auth-material"
    assert env_file.read_text(encoding="utf-8") == original


def test_dash_account_label_suggests_shell_safe_native_env_var(monkeypatch, capsys, private_login):
    monkeypatch.setattr("builtins.input", lambda _prompt="": "my-work")
    session_string_generator.main()

    out = capsys.readouterr().out
    assert "TELEGRAM_SESSION_NAME_MY_WORK=" in out
    assert "TELEGRAM_SESSION_STRING" not in out
    assert "synthetic-auth-material" not in out
    assert "MY-WORK" not in out


def test_login_cancellation_removes_only_new_session(monkeypatch, private_login):
    class CancelledClient(_AuthorizedClient):
        def connect(self):
            raise KeyboardInterrupt

    monkeypatch.setattr(session_string_generator, "TelegramClient", CancelledClient)
    with pytest.raises(KeyboardInterrupt):
        session_string_generator.main()
    assert not private_login.exists()


def test_login_refuses_existing_session_without_connecting(monkeypatch, private_login):
    private_login.parent.mkdir(parents=True, mode=0o700)
    private_login.write_bytes(b"existing-session")
    calls = []
    monkeypatch.setattr(
        session_string_generator, "TelegramClient", lambda *a, **kw: calls.append(a)
    )
    with pytest.raises(SystemExit):
        session_string_generator.main()
    assert private_login.read_bytes() == b"existing-session"
    assert calls == []


def test_login_failure_does_not_echo_auth_material(monkeypatch, capsys, private_login):
    class FailedClient(_AuthorizedClient):
        def connect(self):
            raise RuntimeError("synthetic-auth-material")

    monkeypatch.setattr(session_string_generator, "TelegramClient", FailedClient)
    with pytest.raises(SystemExit):
        session_string_generator.main()
    captured = capsys.readouterr()
    assert "synthetic-auth-material" not in captured.out + captured.err
    assert not private_login.exists()
