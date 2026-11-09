"""Scope before enrichment; one ingress attaches to the existing Telegram owner."""

from datetime import datetime, timezone
from telethon import events
from .models import ScopeTarget, WatchState
from .scope import ScopeDenied, matches_scope
from .store import SourceRecord


def message_topic(message):
    reply = getattr(message, "reply_to", None)
    if reply is not None and getattr(reply, "forum_topic", False):
        return getattr(reply, "reply_to_top_id", None) or getattr(reply, "reply_to_msg_id", None)
    return None


class ScopedIngress:
    def __init__(self, store, *, owner_id, account_id):
        self.store, self.owner_id, self.account_id = store, owner_id, account_id
        self._registrations = []

    def _watches(self, target, kind):
        return tuple(
            w
            for w in self.store.list(self.owner_id)
            if w.state == WatchState.ACTIVE
            and w.active
            and kind in w.active.capabilities
            and matches_scope(w.active.scope, target)
        )

    async def message(self, event, *, edited=False, own_send=False, _watch_id=None):
        msg = event.message
        chat_id = getattr(event, "chat_id", None)
        if not chat_id:
            return ()
        target = ScopeTarget(self.account_id, chat_id, message_topic(msg))
        kind = "telegram.message.edited" if edited else "telegram.message.created"
        watches = tuple(
            w for w in self._watches(target, kind) if _watch_id is None or w.watch_id == _watch_id
        )
        if not watches:
            return ()
        date = getattr(msg, "edit_date", None) if edited else getattr(msg, "date", None)
        if date is None:
            return ()
        metadata = {
            "outgoing": own_send or bool(getattr(msg, "out", False)),
            "mentioned": bool(getattr(msg, "mentioned", False)),
        }
        for key, attr in (("sender_id", "sender_id"), ("grouped_id", "grouped_id")):
            value = getattr(msg, attr, None)
            if type(value) is int:
                metadata[key] = value
        reply = getattr(msg, "reply_to", None)
        reply_id = getattr(reply, "reply_to_msg_id", None)
        if type(reply_id) is int:
            metadata["reply_to_message_id"] = reply_id
        if getattr(msg, "media", None) is not None:
            metadata["media_type"] = "unsupported"
        version = date.isoformat() if edited else "1"
        pts = getattr(getattr(event, "original_update", None), "pts", None)
        if edited and type(pts) is int:
            version = f"{version}:{pts}"
        record = SourceRecord(
            target,
            kind,
            msg.id,
            date,
            version=version,
            metadata=metadata,
        )
        return self._append(watches, record)

    def _append(self, watches, record):
        result = []
        for watch in watches:
            try:
                stored = self.store.append_source(
                    self.owner_id, watch.watch_id, watch.epoch, record
                )
                if stored:
                    result.append(stored)
            except ScopeDenied:
                continue
        return tuple(result)

    async def deleted(self, event):
        kind = "telegram.message.deleted"
        ids = tuple(getattr(event, "deleted_ids", ()) or ())[:100]
        result = []
        # Telegram omits the peer for ordinary private/group deletions. Attribute
        # only known journal entries, never infer a chat from the message ID alone.
        for mid in ids:
            candidates = {}
            for watch in self.store.list(self.owner_id):
                if watch.state != WatchState.ACTIVE or not watch.active:
                    continue
                if kind not in watch.active.capabilities:
                    continue
                for source in self._sources(watch.watch_id):
                    target = source.record.target
                    if (
                        source.epoch == watch.epoch
                        and source.record.message_id == mid
                        and target.account_id == self.account_id
                        and matches_scope(watch.active.scope, target)
                        and (
                            not getattr(event, "chat_id", None)
                            or target.marked_chat_id == event.chat_id
                        )
                    ):
                        candidates.setdefault(target, {})[watch.watch_id] = watch
            if len(candidates) != 1:
                continue
            target, watches = next(iter(candidates.items()))
            record = SourceRecord(target, kind, mid, datetime.now(timezone.utc), metadata={})
            result.extend(self._append(tuple(watches.values()), record))
        return tuple(result)

    def _sources(self, watch_id):
        after = 0
        while True:
            page = self.store.get_sources(self.owner_id, watch_id, after=after, limit=1000)
            if not page:
                return
            yield from page
            after = page[-1].sequence

    async def backfill(self, client, watch_id, target, *, limit=100):
        """Read a bounded recent overlap, stopping at the activation boundary."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Backfill limit must be 1..100")
        watch = self.store.get(self.owner_id, watch_id)
        if (
            not watch
            or watch.state != WatchState.ACTIVE
            or target.account_id != self.account_id
            or not matches_scope(watch.active.scope, target)
        ):
            raise ScopeDenied("Backfill requires an active selected target")
        cursor = self.store.get_checkpoint(self.owner_id, watch_id, target, "messages")
        kwargs = {"limit": limit + 1, "min_id": int(cursor or 0)}
        if target.topic_id is not None:
            kwargs["reply_to"] = target.topic_id
        count, highest = 0, int(cursor or 0)
        try:
            async for msg in client.iter_messages(target.marked_chat_id, **kwargs):
                if msg.date < watch.boundary.replace(microsecond=0):
                    break
                if count >= limit:
                    self.store.mark_gap(self.owner_id, watch_id, watch.epoch, "coverage_gap")
                    break
                current = self.store.get(self.owner_id, watch_id)
                if (
                    not current
                    or current.epoch != watch.epoch
                    or current.state != WatchState.ACTIVE
                ):
                    return count
                from types import SimpleNamespace

                records = await self.message(
                    SimpleNamespace(chat_id=target.marked_chat_id, message=msg), _watch_id=watch_id
                )
                if records:
                    highest = max(highest, msg.id)
                count += 1
            self.store.set_checkpoint(
                self.owner_id, watch_id, watch.epoch, target, "messages", str(highest)
            )
        except Exception as exc:
            from telethon.errors import ChannelPrivateError, ChatAdminRequiredError, FloodWaitError

            reason = "telegram_limit" if isinstance(exc, FloodWaitError) else "coverage_gap"
            self.store.mark_gap(self.owner_id, watch_id, watch.epoch, reason)
            if isinstance(exc, (ChannelPrivateError, ChatAdminRequiredError)):
                self.access_lost(target.marked_chat_id)
        return count

    def access_lost(self, chat_id):
        """Pause and epoch-fence a resource when Telegram confirms lost access."""
        from dataclasses import replace

        for watch in self.store.list(self.owner_id):
            if (
                watch.state == WatchState.ACTIVE
                and watch.active
                and any(
                    t.account_id == self.account_id and t.marked_chat_id == chat_id
                    for t in watch.active.scope.targets
                )
            ):
                self.store.mark_gap(self.owner_id, watch.watch_id, watch.epoch, "coverage_gap")
                self.store.compare_and_swap(
                    self.owner_id,
                    watch.watch_id,
                    watch.version,
                    replace(
                        watch,
                        version=watch.version + 1,
                        epoch=watch.epoch + 1,
                        state=WatchState.PAUSED,
                    ),
                )

    def attach(self, client):
        if self._registrations:
            raise RuntimeError("Ingress already attached")

        async def created(event):
            await self.message(event)

        async def edited(event):
            await self.message(event, edited=True)

        for handler, builder in (
            (created, events.NewMessage()),
            (edited, events.MessageEdited()),
            (self.deleted, events.MessageDeleted()),
        ):
            client.add_event_handler(handler, builder)
            self._registrations.append((client, handler, builder))
        from .reconciliation import bind_ingress

        bind_ingress(client, self)

    def detach(self):
        from .reconciliation import bind_ingress

        for client, handler, builder in self._registrations:
            client.remove_event_handler(handler, builder)
            bind_ingress(client, None)
        self._registrations.clear()
