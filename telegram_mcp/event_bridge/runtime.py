"""Opt-in authenticated composition around the existing Telegram session."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hmac
import json
import logging
import os
from pathlib import Path
import stat

from starlette.responses import JSONResponse


@dataclass(frozen=True)
class Profile:
    id: str
    kind: str
    token: str
    watch_ids: tuple


def private_file(path, *, limit=65536):
    path = Path(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_uid != os.getuid()
            or info.st_size > limit
        ):
            raise ValueError("Private bounded owner file required")
        return stream.read(limit + 1)


def load_profiles(path, expected_owner):
    data = json.loads(private_file(path))
    if data.get("owner_id") != expected_owner or not expected_owner.isdigit():
        raise ValueError("Profile owner must match verified numeric account")
    profiles = []
    for p in data["profiles"]:
        ids = tuple(p.get("watch_ids", ()))
        if (
            p["kind"] not in ("manual", "context")
            or not isinstance(p["id"], str)
            or not p["id"]
            or not 32 <= len(p["token"]) <= 256
            or not p["token"].isascii()
        ):
            raise ValueError("Invalid profile")
        if p["kind"] == "context" and (
            not ids
            or len(ids) > 100
            or any(not isinstance(v, str) or not v or "*" in v for v in ids)
        ):
            raise ValueError("Context requires explicit watches")
        profiles.append(Profile(p["id"], p["kind"], p["token"], ids))
    if (
        len(profiles) > 32
        or sum(p.kind == "manual" for p in profiles) != 1
        or len({p.id for p in profiles}) != len(profiles)
        or len({p.token for p in profiles}) != len(profiles)
    ):
        raise ValueError("Unique profiles and one manual owner required")
    return tuple(profiles)


class ProfileRouter:
    """Authentication covers all HTTP methods and ordinary SDK tools."""

    def __init__(self, profiles, apps, lifespan):
        self.profiles, self.apps, self.lifespan = profiles, apps, lifespan

    async def __call__(self, scope, receive, send):
        if scope["type"] == "lifespan":
            from starlette.applications import Starlette

            return await Starlette(lifespan=self.lifespan)(scope, receive, send)
        if scope["type"] != "http":
            return
        if scope.get("path", "").rstrip("/") != "/mcp":
            return await JSONResponse({"error": "Not found"}, status_code=404)(
                scope, receive, send
            )
        auth = dict(scope.get("headers", ())).get(b"authorization", b"")
        profile = None
        for candidate in self.profiles:
            if hmac.compare_digest(auth, b"Bearer " + candidate.token.encode("ascii")):
                profile = candidate
        if profile is None:
            return await JSONResponse({"error": "Authentication required"}, status_code=401)(
                scope, receive, send
            )
        return await self.apps[profile.id](scope, receive, send)


class EventsRuntime:
    def __init__(self, owner, profiles, store, service, client, *, delivery=False):
        self.owner, self.profiles, self.store, self.service, self.client = (
            owner,
            profiles,
            store,
            service,
            client,
        )
        self.delivery, self.task, self.ingress = delivery, None, None
        self._was_connected = False

    async def _repair(self):
        connected = self.client.is_connected()
        previous = self._was_connected
        self._was_connected = connected
        if connected and not previous:
            remaining = 100
            for watch in self.store.list(self.owner):
                if not watch.active or watch.state.value != "active":
                    continue
                for target in watch.active.scope.targets:
                    if target.account_id != self.owner:
                        continue
                    if remaining <= 0:
                        self.store.mark_gap(
                            self.owner, watch.watch_id, watch.epoch, "coverage_gap"
                        )
                        continue
                    try:
                        count = await asyncio.wait_for(
                            self.ingress.backfill(
                                self.client, watch.watch_id, target, limit=remaining
                            ),
                            timeout=20,
                        )
                        remaining -= max(1, count)
                    except Exception:
                        self.store.mark_gap(
                            self.owner, watch.watch_id, watch.epoch, "coverage_gap"
                        )
                        remaining = 0
        self._was_connected = connected

    async def _loop(self):
        from .delivery import DeliveryWorker

        worker = DeliveryWorker(self.service, owner_id=self.owner)
        while True:
            try:
                await self._repair()
                if self.delivery:
                    await asyncio.wait_for(worker.run_once(), timeout=20)
                now = datetime.now(timezone.utc)
                self.store.prune(self.owner, before=now - timedelta(days=3))
                with self.store._transaction() as db:
                    db.execute(
                        "DELETE FROM analysis_jobs WHERE owner=? AND expires<=?",
                        (self.owner, now.isoformat()),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.getLogger("telegram_mcp").error("Event background work unavailable")
            await asyncio.sleep(5)

    async def start(self):
        from .ingress import ScopedIngress

        self.ingress = ScopedIngress(self.store, owner_id=self.owner, account_id=self.owner)
        self.ingress.attach(self.client)
        self.task = asyncio.create_task(self._loop())

    async def close(self):
        if self.ingress:
            self.ingress.detach()
        if self.task:
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=5)
            except asyncio.CancelledError:
                pass
        self.store.close()


def _open_runtime(env, owner, profiles, client):
    from .protocol import EventService
    from .store import SQLiteStore

    store = service = runtime = None
    try:
        path = Path(env["DOT_EVENTS_DB"])
        parent = path.parent
        if (
            parent.is_symlink()
            or stat.S_IMODE(parent.stat().st_mode) != 0o700
            or parent.stat().st_uid != os.getuid()
        ):
            raise ValueError("Private event directory required")
        if Path(env["DOT_EVENTS_PROFILES_FILE"]).resolve().is_relative_to(parent.resolve()):
            raise ValueError("Profile file must be outside event directory")
        keypath = Path(env["DOT_EVENTS_KEY_FILE"])
        if keypath.resolve().is_relative_to(parent.resolve()):
            raise ValueError("Key must be outside event directory")
        key = private_file(keypath, limit=128).strip()
        if path.exists():
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_uid != os.getuid()
            ):
                raise ValueError("Private database required")
        else:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd)
        store = SQLiteStore(path)

        def authorized(owner_id, profile_id, watch):
            return owner_id == owner and any(
                p.id == profile_id and (p.kind == "manual" or watch in p.watch_ids)
                for p in profiles
            )

        delivery = (
            env.get("DOT_EVENTS_DELIVERY_ENABLED", "0") == "1"
            and env.get("DOT_EVENTS_CONTEXT_VERIFIED", "0") == "1"
        )
        from .protocol import ACTIVITY, ATTENTION, HEALTH

        service = EventService(
            store,
            key=key,
            grant_authorizer=authorized,
            available=(ACTIVITY, ATTENTION, HEALTH) if delivery else (),
        )
        runtime = EventsRuntime(owner, profiles, store, service, client, delivery=delivery)
    except Exception:
        if store:
            store.close()
        logging.getLogger("telegram_mcp").error(
            "Events disabled: private storage or key unavailable"
        )
    return runtime


def _register_controls(manual_mcp, client, owner, store):
    from .tools import WatchManager, register_watch_tools
    from .models import Capability
    from .policies import ReminderPolicy
    from .scope import CAPABILITY_CATALOG
    from telethon import utils
    from telethon.tl.functions.messages import GetForumTopicsByIDRequest

    async def resolve_target(target):
        if target.account_id != owner:
            raise ValueError("Verified account required")
        entity = await client.get_entity(target.marked_chat_id)
        if utils.get_peer_id(entity) != target.marked_chat_id:
            raise ValueError("Canonical marked chat ID required")
        if target.topic_id is not None:
            if not getattr(entity, "forum", False):
                raise ValueError("Forum topic required")
            response = await client(GetForumTopicsByIDRequest(entity, [target.topic_id]))
            if not any(
                getattr(t, "id", None) == target.topic_id and type(t).__name__ == "ForumTopic"
                for t in response.topics
            ):
                raise ValueError("Verified forum topic required")
        return target

    supported = {
        "telegram.message.created",
        "telegram.message.edited",
        "telegram.message.deleted",
    }
    catalog = tuple(
        Capability(
            c.name,
            c.event_class,
            c.name in supported,
            "observed; bounded repair may have gaps" if c.name in supported else c.coverage,
            c.delivery_modes,
        )
        for c in CAPABILITY_CATALOG
    )
    manager = WatchManager(store, owner_id=owner, account_ids=(owner,), catalog=catalog)
    register_watch_tools(
        manual_mcp,
        manager,
        resolve_target=resolve_target,
        reminders=ReminderPolicy(store, owner_id=owner),
    )


def _lifespan(managers, runtime):
    @asynccontextmanager
    async def lifespan(app):
        async with AsyncExitStack() as stack:
            for manager in managers:
                await stack.enter_async_context(manager.run())
            if runtime:
                await runtime.start()
            try:
                yield
            finally:
                if runtime:
                    await runtime.close()

    return lifespan


def configure(manual_mcp, client, *, env=None):
    """Disabled path performs zero file/key/DB/client I/O."""
    env = os.environ if env is None else env
    if env.get("DOT_EVENTS_ENABLED", "0") != "1":
        return None
    from mcp.server.fastmcp import FastMCP
    from .analysis import AnalysisCoordinator, register_analysis_tools
    from .context import ContextProfile, ScopedContextReader
    from .protocol import EventPrincipal
    from telegram_mcp.transport.http import install_events_adapter
    from .store import SQLiteStore

    owner = env.get("DOT_EXPECTED_USER_ID", "")
    # Auth errors fail closed, independently of optional event storage availability.
    profiles = load_profiles(env["DOT_EVENTS_PROFILES_FILE"], owner)
    runtime = _open_runtime(env, owner, profiles, client)
    service, store = (runtime.service, runtime.store) if runtime else (None, None)
    if runtime:
        _register_controls(manual_mcp, client, owner, store)
    managers, apps = [], {}
    for profile in profiles:
        mcp = (
            manual_mcp
            if profile.kind == "manual"
            else FastMCP(
                "Scoped Telegram context",
                stateless_http=True,
                json_response=True,
                transport_security=manual_mcp.settings.transport_security,
            )
        )
        if profile.kind == "context" and runtime:
            reader = ScopedContextReader(
                store, ContextProfile(owner, profile.watch_ids), {owner: client}
            )
            register_analysis_tools(mcp, AnalysisCoordinator(reader, profile.id))
        from mcp.types import ToolAnnotations

        @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, openWorldHint=False))
        def event_runtime_status() -> dict:
            return {
                "events_available": runtime is not None,
                "delivery_enabled": bool(runtime and runtime.delivery),
                "context_host_verified": env.get("DOT_EVENTS_CONTEXT_VERIFIED", "0") == "1",
            }

        app = mcp.streamable_http_app()
        managers.append(mcp.session_manager)
        if service:
            principal = EventPrincipal(
                owner,
                None if profile.kind == "manual" else frozenset(profile.watch_ids),
                profile.id,
            )

            async def resolve(scope, principal=principal):
                return principal

            app = install_events_adapter(
                app,
                service,
                principal_resolver=resolve,
                transport_security=mcp.settings.transport_security,
            )
        apps[profile.id] = app

    return ProfileRouter(profiles, apps, _lifespan(managers, runtime))
