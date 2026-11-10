from __future__ import annotations

import base64
import hashlib
import json
import re
from pathlib import Path

from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import BlobResourceContents, EmbeddedResource, TextContent

from telegram_mcp import runtime
from telegram_mcp.access import AccessDenied, current_connection, current_operation
from telegram_mcp.tools.media import (
    _known_media_size,
    _MediaDownloadError,
    _select_download_target,
    _staged_media_download,
)

# shortcut: this provisional cap leaves room below the 10 MiB tunnel envelope; measure in Dot before raising.
MAX_ORIGINAL_BYTES = 6_000_000
MAX_SERIALIZED_BYTES = 10 * 1024 * 1024


def _attachment_identity(message, target, peer_id, message_id):
    if (
        not message
        or message.id != message_id
        or runtime.get_marked_id(message.peer_id) != peer_id
    ):
        raise ToolError("The requested source is no longer available")
    document = getattr(target, "document", None) or getattr(
        getattr(target, "media", None), "document", None
    )
    photo = getattr(target, "photo", None) or getattr(
        getattr(target, "media", None), "photo", None
    )
    if target is message and not (document or photo):
        raise ToolError("This message has no supported original attachment")
    attachment = document or photo or target
    identifier = getattr(attachment, "id", None)
    if type(identifier) is not int or not identifier:
        raise ToolError("The attachment has no stable source identity")
    return (
        type(attachment).__name__,
        identifier,
        _known_media_size(target),
        getattr(message, "edit_date", None),
        getattr(message, "grouped_id", None),
    ), attachment


def _safe_filename(message, attachment):
    filename = getattr(getattr(message, "file", None), "name", None)
    if not filename:
        filename = next(
            (
                getattr(attribute, "file_name", None)
                for attribute in getattr(attachment, "attributes", [])
                if getattr(attribute, "file_name", None)
            ),
            "attachment.bin",
        )
    filename = Path(str(filename).replace("\\", "/")).name
    filename = "".join(character for character in filename if character.isprintable())[:160]
    return filename if filename not in ("", ".", "..") else "attachment.bin"


async def export_attachment(
    chat_id: int | str,
    message_id: int,
    media_id: str | None = None,
    account: str | None = None,
) -> list[TextContent | EmbeddedResource]:
    """Return exact original bytes from an authorized message as a private MCP resource.

    The connection needs both read and attachment permission for the source. media_id
    selects an embedded rich-post attachment, as a decimal string; omit it for regular
    message media. The provisional maximum is 6,000,000 bytes. This does not claim the
    host can open or analyze the returned format; report unsupported materialization.
    """
    connection = current_connection.get()
    if connection is None or current_operation.get() != "attachments":
        raise AccessDenied("Authenticated attachment operation required")
    if (
        isinstance(chat_id, bool)
        or not isinstance(chat_id, (int, str))
        or (isinstance(chat_id, int) and not -(2**63) <= chat_id < 2**63)
        or type(message_id) is not int
        or not 0 < message_id < 2**31
        or account not in (None, "default")
    ):
        raise ToolError("Invalid source identifier")
    selected_id = None
    if media_id is not None:
        if not isinstance(media_id, str) or not re.fullmatch(r"-?[0-9]{1,19}", media_id):
            raise ToolError("media_id must be a decimal attachment ID string")
        selected_id = int(media_id)
        if not -(2**63) <= selected_id < 2**63 or not selected_id:
            raise ToolError("Invalid attachment ID")
    try:
        connection.revalidate()
        client = runtime.get_client("default")
        entity = await runtime.resolve_entity(chat_id, client)
        peer_id = runtime.get_marked_id(entity)
        source = await client.get_messages(entity, ids=message_id)
        target = _select_download_target(source, message_id, selected_id)
        identity, attachment = _attachment_identity(source, target, peer_id, message_id)
        connection.revalidate()
        root = connection.config.media_root
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        async with _staged_media_download(
            client, target, root, "original", MAX_ORIGINAL_BYTES, "export_attachment", message_id
        ) as downloaded:
            with downloaded.open("rb") as original:
                data = original.read(MAX_ORIGINAL_BYTES + 1)
            if len(data) > MAX_ORIGINAL_BYTES:
                raise ToolError("Original exceeds the attachment byte limit")
            declared_size = identity[2]
            if declared_size is not None and len(data) != declared_size:
                raise ToolError("Original download is incomplete or its declared size changed")
            current = await client.get_messages(entity, ids=message_id)
            current_target = _select_download_target(current, message_id, selected_id)
            current_identity, _ = _attachment_identity(
                current, current_target, peer_id, message_id
            )
            if identity != current_identity:
                raise ToolError("Attachment changed during download; result withheld")
            connection.revalidate()
            if runtime.check_chat_access(peer_id, entity):
                raise AccessDenied("Attachment source permission changed; result withheld")
            mime = getattr(attachment, "mime_type", "")
            if not isinstance(mime, str) or not re.fullmatch(
                r"[A-Za-z0-9!#$&^_.+-]{1,80}/[A-Za-z0-9!#$&^_.+-]{1,80}", mime
            ):
                mime = (
                    "image/jpeg"
                    if isinstance(attachment, runtime.types.Photo)
                    else "application/octet-stream"
                )
            metadata = {
                "source": {
                    "chat_id": str(peer_id),
                    "message_id": message_id,
                    "media_id": str(identity[1]),
                    "album_id": str(identity[4]) if identity[4] is not None else None,
                },
                "filename": _safe_filename(source, attachment),
                "mime_type": mime,
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            }
            content = [
                TextContent(type="text", text=json.dumps(metadata, ensure_ascii=False)),
                EmbeddedResource(
                    type="resource",
                    resource=BlobResourceContents(
                        uri=f"telegram://default/{peer_id}/{message_id}/{identity[1]}",
                        mimeType=mime,
                        blob=base64.b64encode(data).decode("ascii"),
                    ),
                ),
            ]
            if sum(len(item.model_dump_json().encode("utf-8")) for item in content) > (
                MAX_SERIALIZED_BYTES - 4096
            ):
                raise ToolError("Serialized original exceeds the private transport limit")
            return content
    except _MediaDownloadError as error:
        raise ToolError(str(error)) from None
    except (AccessDenied, ToolError):
        raise
    except Exception:
        raise ToolError("Original could not be retrieved; no original was returned") from None
