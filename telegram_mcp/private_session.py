from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path


def default_session_target(name: str) -> str:
    base = Path(os.getenv("XDG_STATE_HOME") or Path.home() / ".local" / "state")
    if not base.is_absolute():
        raise ValueError("XDG_STATE_HOME must be absolute")
    return str(base / "telegram-mcp" / "sessions" / name)


def session_paths(target: str) -> tuple[str, Path]:
    path = Path(os.path.abspath(Path(target).expanduser()))
    if path.name.endswith(".session"):
        path = path.with_name(path.name[: -len(".session")])
    return str(path), Path(f"{path}.session")


def create_private_session(path: Path) -> tuple[int, int]:
    """Reserve an empty 0600 file before Telethon can write authorization material."""
    if os.name == "nt":
        raise ValueError(
            "Private session setup requires POSIX permissions; Windows ACLs are unverified"
        )
    protected = [Path(__file__).resolve().parent.parent]
    roots = os.getenv("TELEGRAM_ALLOWED_ROOTS", "")
    if roots:
        start = 0
        for match in re.finditer(r"[;,:]|$", roots):
            index = match.start()
            if (
                match.group() == ":"
                and index > 0
                and roots[index - 1].isascii()
                and roots[index - 1].isalpha()
            ):
                if index == 1 or roots[index - 2] in ";, \t\r\n\"'":
                    continue
            value = roots[start:index].strip("\"' \t\r\n")
            if value:
                protected.append(Path(value).expanduser().resolve())
            start = index + 1
    config = os.getenv("DOT_ACCESS_CONFIG")
    if config:
        data = json.loads(Path(config).read_text(encoding="utf-8"))
        protected.append(Path(data["media_root"]).resolve())
    resolved = path.resolve()
    if any(
        root == resolved or root in resolved.parents or resolved in root.parents
        for root in protected
    ):
        raise ValueError("Session must be outside the checkout and media roots")
    for item in (path, *path.parents):
        if item.is_symlink():
            if item == path:
                raise FileExistsError("destination already exists")
            raise ValueError("Session directory must not use symlinks")
    for suffix in ("", "-journal", "-wal", "-shm"):
        if os.path.lexists(f"{path}{suffix}"):
            raise FileExistsError("destination or SQLite sidecar already exists")
    for directory in reversed(path.parent.parents):
        if not directory.exists():
            directory.mkdir(mode=0o700)
    path.parent.mkdir(mode=0o700, exist_ok=True)
    info = path.parent.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("Session directory must be owner-only (0700)")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        info = os.fstat(fd)
        return info.st_dev, info.st_ino
    finally:
        os.close(fd)


def remove_created_session(path: Path, identity: tuple[int, int]) -> None:
    """Remove a failed new session, never a preexisting or replaced destination."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != identity:
        return
    for suffix in ("-journal", "-wal", "-shm", ""):
        candidate = Path(f"{path}{suffix}")
        if candidate.is_file() and not candidate.is_symlink():
            candidate.unlink()
