"""Inspect disposable distributions containing only synthetic private fixtures."""

import os
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
MARKER = b"synthetic-private-material-not-for-distribution-582901"
PRIVATE_PATHS = (
    ".env",
    ".env.backup",
    "telegram.env.bak",
    "auth.session",
    "auth.session-journal",
    "auth.session-wal",
    "auth.session-shm",
    "auth.session.backup",
    "state/private.txt",
    "transcripts/private.txt",
    "cache/private.txt",
    ".cache/private.txt",
    "logs/private.txt",
    "data/private.txt",
    "media/private.png",
    "audio/private.wav",
    "backups/private.txt",
    "aliases.json",
    "incoming_feed.jsonl",
    "mcp_errors.log",
    "private.bak",
    "private.backup",
    "private~",
    "transcripts.db-wal",
    "access.json",
)


@pytest.fixture
def disposable_project(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    for name in (
        "pyproject.toml",
        "uv.lock",
        "README.md",
        "LICENSE",
        "MANIFEST.in",
        "main.py",
        "migrate_session.py",
        "sanitize.py",
        "session_string_generator.py",
        "manifest.json",
        ".gitignore",
        ".dockerignore",
        ".mcpbignore",
    ):
        shutil.copyfile(ROOT / name, project / name)
    for name in ("Dockerfile", "Converter.Dockerfile", "dot_server.py"):
        target = project / "deploy/dot" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / "deploy/dot" / name, target)
    for source in (ROOT / "telegram_mcp").rglob("*.py"):
        target = project / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    for prefix in ("", "telegram_mcp/", "telegram_mcp/tools/"):
        for name in PRIVATE_PATHS:
            target = project / f"{prefix}{name}"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(MARKER)
    return project


def _assert_clean_archive(archive):
    if archive.suffix == ".whl" or archive.suffix == ".mcpb":
        with zipfile.ZipFile(archive) as contents:
            members = {
                name: contents.read(name) for name in contents.namelist() if not name.endswith("/")
            }
    else:
        with tarfile.open(archive) as contents:
            members = {
                item.name: contents.extractfile(item).read() for item in contents if item.isfile()
            }
    assert members
    assert all(MARKER not in data for data in members.values()), archive.name
    assert any(name.endswith("telegram_mcp/private_session.py") for name in members)
    return members


def test_git_excludes_synthetic_private_files_without_staging(disposable_project):
    git_dir = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--absolute-git-dir"],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    candidates = [
        str(path.relative_to(disposable_project))
        for path in disposable_project.rglob("*")
        if path.is_file() and path.read_bytes() == MARKER
    ]
    result = subprocess.run(
        [
            "git",
            "--git-dir",
            git_dir,
            "--work-tree",
            str(disposable_project),
            "-c",
            "core.excludesFile=/dev/null",
            "check-ignore",
            "--no-index",
            "--stdin",
        ],
        input="\n".join(candidates) + "\n",
        text=True,
        capture_output=True,
        check=True,
    )
    assert set(result.stdout.splitlines()) == set(candidates)


def test_built_wheel_and_sdist_exclude_private_material(disposable_project, tmp_path):
    output = tmp_path / "dist"
    result = subprocess.run(
        [
            "uv",
            "build",
            "--offline",
            "--python",
            sys.executable,
            "--out-dir",
            str(output),
            str(disposable_project),
        ],
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    artifacts = sorted(path for path in output.iterdir() if path.suffix in (".whl", ".gz"))
    assert len(artifacts) == 2
    for archive in artifacts:
        _assert_clean_archive(archive)


def test_official_mcp_bundle_excludes_private_material(disposable_project, tmp_path):
    executable = os.getenv("MCPB_TEST_CLI") or shutil.which("mcpb")
    if not executable:
        pytest.skip("Official MCP bundle CLI is unavailable; no bundle packaging claim")
    output = tmp_path / "telegram.mcpb"
    result = subprocess.run(
        [executable, "pack", str(disposable_project), str(output)],
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    _assert_clean_archive(output)


def test_docker_context_excludes_private_material(disposable_project, tmp_path):
    if os.getenv("DOT_DOCKER_TESTS") != "1":
        pytest.skip("Set DOT_DOCKER_TESTS=1 with an isolated Docker builder")
    output = tmp_path / "context"
    result = subprocess.run(
        [
            "docker",
            "build",
            "--file",
            "-",
            "--output",
            f"type=local,dest={output}",
            str(disposable_project),
        ],
        input="FROM scratch\nCOPY . /\n",
        text=True,
        capture_output=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    files = [path for path in output.rglob("*") if path.is_file()]
    assert files
    assert all(MARKER not in path.read_bytes() for path in files)
    assert (output / "deploy/dot/Converter.Dockerfile").is_file()
    assert (output / "telegram_mcp/audio_converter.py").is_file()


def test_compose_resolves_private_mounts_and_loopback_limits(tmp_path):
    if not shutil.which("docker"):
        pytest.skip("Docker Compose CLI is unavailable")
    env_file = tmp_path / "telegram.env"
    shutil.copyfile(ROOT / "deploy/dot/.env.example", env_file)
    environment = {
        **os.environ,
        "TELEGRAM_DOT_IMAGE": "example.invalid/telegram-dot:synthetic",
        "TELEGRAM_DOT_CONVERTER_IMAGE": "example.invalid/telegram-dot-converter:synthetic",
        "TELEGRAM_DOT_ENV_FILE": str(env_file),
        "TELEGRAM_DOT_SESSION_DIR": str(tmp_path / "session"),
        "TELEGRAM_DOT_CONFIG_DIR": str(tmp_path / "config"),
        "TELEGRAM_DOT_STATE_DIR": str(tmp_path / "state"),
    }
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "deploy/dot/compose.yaml"),
            "config",
            "--format",
            "json",
        ],
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    service = json.loads(result.stdout)["services"]["telegram-mcp"]
    assert service["user"] == "1000:1000"
    assert all(port["host_ip"] == "127.0.0.1" for port in service["ports"])
    assert int(service["mem_limit"]) == int(service["memswap_limit"]) == 384 * 1024**2
    assert float(service["cpus"]) == 0.25
    assert service["pids_limit"] == 64
    assert service["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in service["security_opt"]
    assert service["environment"]["TELEGRAM_EVENT_FEED"] == "0"
    assert service["environment"]["TELEGRAM_LEGACY_EVENTS"] == "0"
    assert service["environment"]["TELEGRAM_AUDIO_SCRATCH_DIR"] == "/app/state/audio"
    mounts = {mount["target"]: mount for mount in service["volumes"]}
    assert set(mounts) == {"/app/session", "/app/config", "/app/state"}
    assert mounts["/app/config"]["read_only"] is True
    assert all(mount["bind"]["create_host_path"] is False for mount in mounts.values())
    converter = json.loads(result.stdout)["services"]["audio-converter"]
    assert converter["network_mode"] == "none" and converter["read_only"] is True
    assert converter["user"] == "1000:1000"
    assert converter["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in converter["security_opt"]
    assert len(converter["volumes"]) == 1
    assert converter["volumes"][0]["target"] == "/audio"
    assert converter["volumes"][0]["source"] == str(tmp_path / "state/audio")
    assert not converter.get("ports") and not converter.get("env_file")
    assert not converter.get("environment")
    assert int(converter["mem_limit"]) == int(converter["memswap_limit"]) == 384 * 1024**2
    assert float(converter["cpus"]) == 0.25 and converter["pids_limit"] == 64


def test_owner_example_fails_closed_then_defaults_to_saved_messages(tmp_path):
    from telegram_mcp.access import AccessDenied, load_access_config

    config = json.loads((ROOT / "deploy/dot/access.example.json").read_text(encoding="utf-8"))
    config["media_root"] = str(tmp_path / "media")
    path = tmp_path / "access.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(AccessDenied):
        load_access_config(path)
    config["profiles"]["dot"]["token"] = "synthetic-backend-token-for-tests-582901"
    path.write_text(json.dumps(config), encoding="utf-8")
    profile = load_access_config(path).profiles[0]
    assert profile.read == frozenset({config["owner_id"]})
    assert profile.send == profile.attachments == profile.external_asr == frozenset()


def test_bundle_native_session_setting_preserves_optional_legacy_input():
    manifest = json.loads((ROOT / "manifest.json").read_text(encoding="utf-8"))
    config = manifest["user_config"]
    for name in ("session_name", "session_string"):
        assert config[name]["required"] is False
        assert config[name]["default"] == ""
    assert config["session_string"]["sensitive"] is True
    environment = manifest["server"]["mcp_config"]["env"]
    assert environment["TELEGRAM_SESSION_NAME"] == "${user_config.session_name}"
    assert environment["TELEGRAM_SESSION_STRING"] == "${user_config.session_string}"
