"""Observe accepted local sends without changing their result or retrying Telegram."""

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from weakref import WeakKeyDictionary
from telethon import utils

_bindings = WeakKeyDictionary()


def bind_ingress(client, ingress):
    if ingress is None:
        _bindings.pop(client, None)
    else:
        _bindings[client] = ingress


async def observe_send(client, entity, result, *, reply_to=None):
    ingress = _bindings.get(client)
    if ingress is None:
        return
    try:
        chat_id = utils.get_peer_id(entity)
        message = next(
            (
                getattr(u, "message", None)
                for u in getattr(result, "updates", ())
                if getattr(getattr(u, "message", None), "id", None)
            ),
            None,
        )
        if message is None:
            message = result if getattr(result, "id", None) else None
        if message is None or not getattr(message, "date", None):
            # Acceptance without a full message needs selected history repair;
            # there is deliberately no automatic second send.
            target_watches = [
                w
                for w in ingress.store.list(ingress.owner_id)
                if w.active
                and any(
                    t.account_id == ingress.account_id and t.marked_chat_id == chat_id
                    for t in w.active.scope.targets
                )
            ]
            for watch in target_watches:
                ingress.store.mark_gap(
                    ingress.owner_id, watch.watch_id, watch.epoch, "reconciliation_required"
                )
            return
        if reply_to is not None and not getattr(message, "reply_to", None):
            for watch in ingress.store.list(ingress.owner_id):
                if watch.active and any(
                    t.account_id == ingress.account_id
                    and t.marked_chat_id == chat_id
                    and t.topic_id is not None
                    for t in watch.active.scope.targets
                ):
                    ingress.store.mark_gap(
                        ingress.owner_id, watch.watch_id, watch.epoch, "reconciliation_required"
                    )
        await ingress.message(SimpleNamespace(chat_id=chat_id, message=message), own_send=True)
    except Exception:
        logging.getLogger("telegram_mcp").error("Accepted send event could not be recorded")
