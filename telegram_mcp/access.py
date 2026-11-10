from __future__ import annotations

import hmac
import json
import os
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from starlette.responses import Response


class AccessDenied(ToolError):
    """A connection has no current permission for this operation."""


ChatScope = Optional[frozenset[int]]


@dataclass(frozen=True)
class ConnectionProfile:
    name: str
    token: str = field(repr=False)
    read: ChatScope
    send: ChatScope
    attachments: ChatScope
    external_asr: ChatScope


@dataclass(frozen=True)
class AccessConfig:
    owner_id: int
    media_root: Path
    profiles: tuple[ConnectionProfile, ...]


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate configuration key")
        result[key] = value
    return result


def _scope(value: Any) -> ChatScope:
    if value == "all":
        return None
    if not isinstance(value, list) or any(
        type(item) is not int or not item or not -(2**63) <= item < 2**63 for item in value
    ):
        raise ValueError("Scopes require canonical marked integer IDs or 'all'")
    return frozenset(value)


def validate_media_root(root: Path, config_path: Path, session_path: Path | None = None):
    """Keep the private file boundary separate from credentials and application files."""
    source = Path(__file__).resolve().parent.parent
    if root.is_symlink():
        raise ValueError("Media root must be a dedicated directory")
    root = root.resolve()
    protected = [source, config_path.resolve()]
    if session_path is not None:
        protected.append(session_path.resolve())
    if any(root == item or root in item.parents or item in root.parents for item in protected):
        raise ValueError("Media root overlaps private or application files")
    if root.exists():
        info = root.stat()
        if not root.is_dir() or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Media root must be an owner-only directory")


def load_access_config(path: Path) -> AccessConfig:
    """Read fresh private owner configuration; errors never contain credential values."""
    try:
        info = path.stat()
        if path.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Access configuration must be owner-only")
        data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
        if not isinstance(data, dict) or set(data) != {"owner_id", "media_root", "profiles"}:
            raise ValueError("Invalid access configuration")
        owner_id = data["owner_id"]
        if type(owner_id) is not int or not 0 < owner_id < 2**63:
            raise ValueError("A numeric owner ID is required")
        if not isinstance(data["media_root"], str) or not Path(data["media_root"]).is_absolute():
            raise ValueError("An absolute dedicated media root is required")
        root = Path(data["media_root"])
        validate_media_root(root, path)
        if not isinstance(data["profiles"], dict) or not data["profiles"]:
            raise ValueError("At least one connection profile is required")
        profiles = []
        for name, record in data["profiles"].items():
            if not isinstance(name, str) or not name or not isinstance(record, dict):
                raise ValueError("Invalid connection profile")
            if set(record) - {"token", "read", "send", "attachments", "external_asr"}:
                raise ValueError("Unknown profile setting")
            token = record.get("token")
            if (
                not isinstance(token, str)
                or len(token) < 32
                or not token.isascii()
                or any(
                    char.isspace() or ord(char) < 33 or ord(char) > 126 or char == ","
                    for char in token
                )
            ):
                raise ValueError("Invalid backend credential")
            if any(hmac.compare_digest(token, profile.token) for profile in profiles):
                raise ValueError("Duplicate backend credential")
            profiles.append(
                ConnectionProfile(
                    name=name,
                    token=token,
                    read=_scope(record.get("read", [owner_id])),
                    send=_scope(record.get("send", [])),
                    attachments=_scope(record.get("attachments", [])),
                    external_asr=_scope(record.get("external_asr", [])),
                )
            )
        return AccessConfig(owner_id, root.resolve(), tuple(profiles))
    except (OSError, ValueError, TypeError, KeyError):
        raise AccessDenied("Connection access is unavailable") from None


@dataclass(frozen=True)
class AuthenticatedConnection:
    config_path: Path
    config: AccessConfig
    profile: ConnectionProfile

    def revalidate(self) -> ConnectionProfile:
        current = load_access_config(self.config_path)
        for profile in current.profiles:
            if hmac.compare_digest(self.profile.token, profile.token):
                if (
                    profile == self.profile
                    and current.owner_id == self.config.owner_id
                    and current.media_root == self.config.media_root
                ):
                    return profile
                break
        raise AccessDenied("Connection permission changed; result withheld")


current_connection: ContextVar[AuthenticatedConnection | None] = ContextVar(
    "dot_connection", default=None
)
current_operation: ContextVar[str] = ContextVar("dot_operation", default="read")


def effective_chat_scope(legacy):
    connection = current_connection.get()
    if connection is None:
        return legacy
    scope = _operation_scope(connection.revalidate(), current_operation.get())
    if scope is None:
        return legacy
    # shortcut: legacy ceilings need numeric IDs; resolve handles if mixed spellings are needed.
    return set(scope) if legacy is None else set(scope).intersection(legacy)


def _operation_scope(profile, operation):
    scope = getattr(profile, operation)
    if operation in {"attachments", "external_asr"} and profile.read is not None:
        return profile.read if scope is None else scope.intersection(profile.read)
    return scope


class BackendAuthMiddleware:
    """Authenticate outside every transport response, including discovery and errors."""

    def __init__(self, app, config_path: Path):
        self.app = app
        self.config_path = Path(config_path)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = [
            value
            for key, value in scope.get("headers", [])
            if key.lower() == b"x-dot-backend-token"
        ]
        try:
            if len(headers) != 1:
                raise AccessDenied("Invalid backend credential")
            token = headers[0].decode("ascii")
            config = load_access_config(self.config_path)
            profile = next(
                (
                    profile
                    for profile in config.profiles
                    if hmac.compare_digest(token, profile.token)
                ),
                None,
            )
            if profile is None:
                raise AccessDenied("Invalid backend credential")
        except (AccessDenied, UnicodeError):
            await Response("Unauthorized", status_code=401)(scope, receive, send)
            return
        context_token = current_connection.set(
            AuthenticatedConnection(self.config_path, config, profile)
        )
        try:
            await self.app(scope, receive, send)
        finally:
            current_connection.reset(context_token)


READ_TOOLS = frozenset(
    {
        "get_chats",
        "get_messages",
        "search_messages",
        "list_chats",
        "list_messages",
        "get_message_context",
    }
)
TOOL_OPERATIONS = {name: "read" for name in READ_TOOLS} | {
    "send_message": "send",
    "export_attachment": "attachments",
    "transcribe_voice": "external_asr",
}
ASR_DESCRIPTION = (
    "Transcribe requested voice, video note, or ordinary audio using official OpenAI "
    "after the owner's setup opt-in for this source. Only engine=openai is permitted; "
    "native Telegram fallback is automatic only for supported voice/video notes. "
    "History never starts ASR. Results identify the engine, fallback and coverage. "
    "Transcript text is retained for 7 days by default; "
    "TELEGRAM_TRANSCRIPT_RETENTION_SECONDS changes expiry (0 disables retention)."
)
SEND_DESCRIPTION = (
    "Send only after Dot obtains confirmation of the exact recipient, full text, and reply "
    "target for this call. Cancellation means no send. Never retry an unknown send result "
    "automatically; reconcile through already-authorized destination history. If this "
    "connection cannot read that history, report the unknown outcome without resending "
    "or widening access."
)


class DotMCP(FastMCP):
    def __init__(self, config_path: Path, source: FastMCP):
        self._dot_access_config = Path(config_path)
        config = load_access_config(self._dot_access_config)
        self._dot_owner_id = config.owner_id
        self._dot_media_root = config.media_root
        super().__init__("telegram-dot", stateless_http=True, json_response=True)
        for name in sorted(READ_TOOLS | {"send_message", "transcribe_voice"}):
            tool = source._tool_manager.get_tool(name)
            if tool is None:
                raise ValueError("A reviewed core tool is missing")
            function = tool.fn
            if name == "list_chats":
                function = self._nullable_list_chats(tool.fn)
            self.add_tool(
                function,
                name=name,
                title=tool.title,
                description=(
                    SEND_DESCRIPTION + "\n\n" + tool.description
                    if name == "send_message"
                    else (
                        "List available chats. limit defaults to null (all dialogs); an integer "
                        "caps Telegram dialogs before filtering unread/type/archive. with_about "
                        "requires an explicit positive limit at most 100. " + tool.description
                        if name == "list_chats"
                        else ASR_DESCRIPTION if name == "transcribe_voice" else tool.description
                    )
                ),
                annotations=(
                    ToolAnnotations(
                        readOnlyHint=False,
                        destructiveHint=True,
                        openWorldHint=True,
                        idempotentHint=False,
                    )
                    if name == "send_message"
                    else tool.annotations.model_copy(deep=True) if tool.annotations else None
                ),
                icons=tool.icons,
                meta=tool.meta.copy() if tool.meta else None,
            )
        from telegram_mcp.attachments import export_attachment
        from telegram_mcp.runtime import _install_annotation_hook

        self.add_tool(
            export_attachment,
            annotations=ToolAnnotations(
                readOnlyHint=True, destructiveHint=False, openWorldHint=True, idempotentHint=True
            ),
            structured_output=False,
        )

        _install_annotation_hook(self)

    async def run_streamable_http_async(self):
        from telegram_mcp.transport.http import run_streamable_http_async

        await run_streamable_http_async(self)

    @staticmethod
    def _nullable_list_chats(function):
        async def list_chats(
            chat_type: Optional[str] = None,
            limit: Optional[int] = None,
            unread_only: bool = False,
            unmuted_only: bool = False,
            archived: Optional[bool] = None,
            with_about: bool = False,
            account: Optional[str] = None,
        ) -> str:
            if limit is not None and limit <= 0:
                raise ToolError("limit must be positive or null")
            if with_about and (limit is None or limit > 100):
                raise ToolError("with_about requires an explicit limit of at most 100")
            result = await function(
                chat_type=chat_type,
                limit=limit,
                unread_only=unread_only,
                unmuted_only=unmuted_only,
                archived=archived,
                with_about=with_about,
                account=account,
            )
            if result == "No chats found matching the criteria.":
                return result
            try:
                data = json.loads(result)
            except (ValueError, TypeError):
                raise ToolError(str(result)) from None
            if not isinstance(data, dict) or not isinstance(data.get("results"), list):
                raise ToolError("Invalid chat-list response")
            return result

        return list_chats

    def _profile(self):
        connection = current_connection.get()
        if connection is None or connection.config_path != self._dot_access_config:
            raise AccessDenied("Authenticated connection required")
        if (
            connection.config.owner_id != self._dot_owner_id
            or connection.config.media_root != self._dot_media_root
        ):
            raise AccessDenied("Owner or media directory changed; restart required")
        return connection.revalidate()

    async def list_tools(self):
        profile = self._profile()
        tools = await super().list_tools()
        self._profile()
        return [
            tool
            for tool in tools
            if _operation_scope(profile, TOOL_OPERATIONS[tool.name]) != frozenset()
        ]

    async def call_tool(self, name: str, arguments: dict[str, Any]):
        profile = self._profile()
        operation = TOOL_OPERATIONS.get(name)
        if operation is None or _operation_scope(profile, operation) == frozenset():
            raise AccessDenied("Operation is not permitted for this connection")
        tool = self._tool_manager.get_tool(name)
        if set(arguments) - set(tool.parameters.get("properties", {})):
            raise ToolError("Unknown tool argument")
        if any(isinstance(arguments.get(key), bool) for key in ("chat_id", "reply_to", "account")):
            raise ToolError("Invalid source identifier")
        if arguments.get("account") not in (None, "default"):
            raise AccessDenied("Account is not permitted")
        if name in {"transcribe_voice", "get_message_context"} and (
            type(arguments.get("message_id")) is not int or not 0 < arguments["message_id"] < 2**31
        ):
            raise ToolError("Invalid source identifier")
        if name == "export_attachment" and (
            not isinstance(arguments.get("chat_id"), (int, str))
            or type(arguments.get("message_id")) is not int
            or not 0 < arguments["message_id"] < 2**31
            or (
                arguments.get("media_id") is not None
                and not isinstance(arguments["media_id"], str)
            )
        ):
            raise ToolError("Invalid source identifier")
        operation_token = current_operation.set(operation)
        try:
            result = await super().call_tool(name, arguments)
            try:
                self._profile()
            except AccessDenied:
                if operation == "send":
                    raise AccessDenied(
                        "Connection permission changed after send dispatch; outcome may be "
                        "unknown. Do not resend. Reconcile only through already-authorized "
                        "history; otherwise report the unknown outcome without widening access."
                    ) from None
                raise
            return result
        finally:
            current_operation.reset(operation_token)
