from dataclasses import dataclass
import hashlib
import hmac
import base64
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken
from starlette.responses import JSONResponse

from .callbacks import CallbackClient, CallbackError, decode_secret, validate_callback_url
from .models import WatchState
from .scope import ScopeDenied
from .store import DeliveryRequest, _json as canonical

ACTIVITY = "telegram.activity.batch_ready"
ATTENTION = "telegram.attention.due"
HEALTH = "telegram.watch.health_changed"


class ProtocolError(ValueError):
    def __init__(self, message, code=-32602, reason=None):
        super().__init__(message)
        self.code, self.reason = code, reason


@dataclass(frozen=True)
class EventPrincipal:
    owner_id: str
    allowed_watch_ids: frozenset | None = None
    profile_id: str | None = None

    def __post_init__(self):
        if not isinstance(self.owner_id, str) or not self.owner_id.strip():
            raise ValueError("Authenticated owner required")
        if self.allowed_watch_ids is not None:
            object.__setattr__(self, "allowed_watch_ids", frozenset(self.allowed_watch_ids))


class EventService:
    """Key is supplied by deployment outside the SQLite database.

    Only implemented publishers are advertised. Attention and health can be enabled
    when their budgeted publishers are installed; no raw catalog entries appear.
    """

    def __init__(
        self,
        store,
        *,
        key,
        callbacks=None,
        clock=lambda: datetime.now(timezone.utc),
        available=(ACTIVITY,),
        grant_authorizer=None,
    ):
        self.store, self.cipher, self.callbacks, self.clock = (
            store,
            Fernet(key),
            callbacks or CallbackClient(),
            clock,
        )
        self.grant_authorizer = grant_authorizer
        self.cursor_key = hashlib.sha256(key + b"events-cursor").digest()
        self.available = tuple(
            dict.fromkeys(n for n in available if n in (ACTIVITY, ATTENTION, HEALTH))
        )

    def list_events(self):
        return {
            "events": [
                dict(
                    name=name,
                    description={
                        ACTIVITY: "A settled batch of scoped Telegram changes.",
                        ATTENTION: "An explicit reminder requires fresh scoped analysis.",
                        HEALTH: "Coalesced watch coverage status.",
                    }[name],
                    delivery=["webhook"],
                    inputSchema={
                        "type": "object",
                        "required": ["watch_id", "scope_version"],
                        "additionalProperties": False,
                        "properties": {
                            "watch_id": {"type": "string"},
                            "scope_version": {"type": "integer", "minimum": 1},
                        },
                    },
                    payloadSchema=self._payload_schema(name),
                )
                for name in self.available
            ]
        }

    @staticmethod
    def _payload_schema(name):
        properties = {
            "watch_id": {"type": "string"},
            "scope_version": {"type": "integer"},
            "epoch": {"type": "integer"},
        }
        fields = {
            ACTIVITY: {
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "sequences": {"type": "array", "items": {"type": "integer"}},
                "has_more": {"type": "boolean"},
            },
            ATTENTION: {
                "recheck_required": {"const": True},
                "job_id": {"type": "string"},
                "obligation_id": {"type": "string"},
            },
            HEALTH: {
                "status": {"type": "string"},
                "reason": {"type": "string"},
                "coverage_gaps": {"type": "integer"},
            },
        }[name]
        properties.update(fields)
        return {
            "type": "object",
            "properties": properties,
            "required": list(properties),
            "additionalProperties": False,
        }

    def _identity(self, owner, params):
        name, args, delivery = params.get("name"), params.get("arguments"), params.get("delivery")
        if name not in self.available:
            raise ProtocolError("Event is unavailable")
        if (
            not isinstance(args, dict)
            or set(args) != {"watch_id", "scope_version"}
            or not isinstance(args["watch_id"], str)
            or type(args["scope_version"]) is not int
        ):
            raise ProtocolError("An exact watch and scope revision are required")
        if not isinstance(delivery, dict) or delivery.get("mode") != "webhook":
            raise ProtocolError("Webhook delivery is required")
        try:
            url = validate_callback_url(delivery.get("url"))
        except (ValueError, TypeError):
            raise ProtocolError("Invalid callback URL") from None
        identity = hashlib.sha256(canonical((owner, url, name, args)).encode()).hexdigest()
        recipient = hashlib.sha256(canonical((owner, url, name)).encode()).hexdigest()
        return identity, recipient, url, name, args

    def _watch(self, owner, args):
        watch = self.store.get(owner, args["watch_id"])
        if (
            not watch
            or watch.state != WatchState.ACTIVE
            or not watch.active
            or watch.active.revision != args["scope_version"]
            or not watch.active.policy.wake_dot
        ):
            raise ProtocolError("Owned active watch and exact scope revision required")
        return watch

    def _decode(self, row):
        try:
            return json.loads(self.cipher.decrypt(row["document"]))
        except (InvalidToken, ValueError):
            raise ProtocolError("Subscription key is unavailable", -32603) from None

    def _authorized(self, owner, profile_id, watch_id):
        return profile_id is None or (
            self.grant_authorizer is not None
            and self.grant_authorizer(owner, profile_id, watch_id) is True
        )

    def subscriptions(self, owner, watch_id=None):
        now = self.clock()
        result = []
        for row in self.store.subscription_rows(owner, watch_id):
            watch = self.store.get(owner, row["watch"])
            if datetime.fromisoformat(row["expires"]) <= now:
                self.store.remove_subscription(owner, row["identity"], now=now)
                continue
            if not watch or watch.state != WatchState.ACTIVE or watch.epoch != row["epoch"]:
                continue
            grant = self._decode(row)
            if not self._authorized(owner, grant.get("profile_id"), row["watch"]):
                self.store.remove_subscription(owner, row["identity"], now=now)
                continue
            result.append(grant)
        return tuple(result)

    def get_subscription(self, owner, subscription):
        return next((s for s in self.subscriptions(owner) if s["id"] == subscription), None)

    def cursor(self, grant, position):
        body = base64.urlsafe_b64encode(
            canonical(
                {
                    "owner": hashlib.sha256(grant["owner"].encode()).hexdigest(),
                    "identity": grant["identity"],
                    "epoch": grant["epoch"],
                    "position": position,
                }
            ).encode()
        ).decode()
        signature = hmac.new(self.cursor_key, body.encode(), hashlib.sha256).hexdigest()
        return body + "." + signature

    def read_cursor(self, grant, cursor):
        try:
            if not isinstance(cursor, str) or len(cursor) > 2048:
                raise ValueError()
            body, signature = cursor.split(".")
            if not hmac.compare_digest(
                signature, hmac.new(self.cursor_key, body.encode(), hashlib.sha256).hexdigest()
            ):
                raise ValueError()
            data = json.loads(base64.urlsafe_b64decode(body))
            if (
                (data["owner"], data["identity"], data["epoch"])
                != (
                    hashlib.sha256(grant["owner"].encode()).hexdigest(),
                    grant["identity"],
                    grant["epoch"],
                )
                or type(data["position"]) is not int
                or data["position"] < 0
            ):
                raise ValueError()
            return data["position"]
        except (ValueError, KeyError, TypeError, AttributeError):
            raise ProtocolError("Cursor does not belong to this grant and epoch") from None

    async def subscribe(self, owner, params, *, profile_id=None):
        if not owner:
            raise ProtocolError("Authentication required", -32001)
        identity, recipient, url, name, args = self._identity(owner, params)
        watch = self._watch(owner, args)
        if not self._authorized(owner, profile_id, watch.watch_id):
            raise ProtocolError("Profile no longer authorizes this watch", -32001)
        secret = params["delivery"].get("secret")
        try:
            decode_secret(secret)
        except (ValueError, TypeError):
            raise ProtocolError("Invalid webhook secret") from None
        ttl = params.get("ttlMs", 86400000)
        if ttl is None:
            ttl = 86400000
        if type(ttl) is not int or ttl <= 0:
            raise ProtocolError("A positive TTL is required")
        ttl = min(ttl, 86400000)
        existing = next(
            (g for g in self.subscriptions(owner, args["watch_id"]) if g["identity"] == identity),
            None,
        )
        expected_document = next(
            (
                row["document"]
                for row in self.store.subscription_rows(owner, args["watch_id"])
                if row["identity"] == identity
            ),
            None,
        )
        watch = self._watch(owner, args)
        grant = (
            existing.copy()
            if existing
            else dict(
                id=str(uuid4()),
                owner=owner,
                identity=identity,
                recipient=recipient,
                url=url,
                name=name,
                arguments=args,
                epoch=watch.epoch,
            )
        )
        if params.get("cursor") is not None:
            position = self.read_cursor(grant, params["cursor"])
            if position > self.store.event_position(owner, grant["id"]):
                raise ProtocolError("Cursor exceeds the receipt frontier")
        else:
            position = self.store.event_position(owner, grant["id"])
        try:
            await self.callbacks.verify(owner, url, secret, grant["id"])
        except CallbackError as exc:
            raise ProtocolError("Callback verification failed", -32015, exc.reason) from None
        now = self.clock()
        expiry = now + timedelta(milliseconds=ttl)
        if existing and existing["secret"] != secret:
            grant.update(
                previous_secret=existing["secret"],
                rotation_until=(now + timedelta(minutes=5)).isoformat(),
            )
        grant.update(secret=secret, expires=expiry.isoformat(), profile_id=profile_id)
        try:
            if not self._authorized(owner, profile_id, watch.watch_id):
                raise ProtocolError("Profile no longer authorizes this watch", -32001)
            current = self._watch(owner, args)
            if current.epoch != watch.epoch:
                raise ProtocolError("Watch changed during callback verification")
            self.store.save_subscription(
                owner,
                identity,
                grant["id"],
                watch.watch_id,
                watch.epoch,
                expiry,
                self.cipher.encrypt(canonical(grant).encode()),
                expected_document=expected_document,
            )
        except ScopeDenied:
            raise ProtocolError("Watch changed during callback verification") from None
        truncated = False
        if params.get("cursor") is not None:
            truncated = self.store.request_replay(owner, grant["id"], position, now=now)
        return dict(
            id=grant["id"],
            refreshBefore=expiry.isoformat(),
            cursor=self.cursor(grant, position),
            truncated=truncated,
        )

    async def unsubscribe(self, owner, params):
        if not owner:
            raise ProtocolError("Authentication required", -32001)
        identity, *_ = self._identity(owner, params)
        self.store.remove_subscription(owner, identity, now=self.clock())
        return {}

    def activity_requests(self, owner, watch_id):
        return tuple(
            DeliveryRequest(s["recipient"], s["id"])
            for s in self.subscriptions(owner, watch_id)
            if s["name"] == ACTIVITY
        )

    async def dispatch(self, principal, method, params):
        if not isinstance(principal, EventPrincipal):
            raise ProtocolError("Authentication required", -32001)
        owner = principal.owner_id
        if not isinstance(params, dict):
            raise ProtocolError("Object parameters required")
        if method != "events/list" and principal.allowed_watch_ids is not None:
            arguments = params.get("arguments")
            if (
                not isinstance(arguments, dict)
                or arguments.get("watch_id") not in principal.allowed_watch_ids
            ):
                raise ProtocolError("Watch is outside the authenticated profile", -32001)
        if not owner:
            raise ProtocolError("Authentication required", -32001)
        if method == "events/list":
            return self.list_events()
        if method == "events/subscribe":
            return await self.subscribe(owner, params, profile_id=principal.profile_id)
        if method == "events/unsubscribe":
            return await self.unsubscribe(owner, params)
        raise ProtocolError("Method not found", -32601)


class EventsMiddleware:
    """Only events and initialization are adapted around the pinned SDK.

    principal_resolver must validate the same connection credentials as tools.
    It receives ASGI scope, never JSON-RPC parameters. No anonymous fallback exists.
    """

    def __init__(self, app, service, *, principal_resolver, transport_security, mcp_path="/mcp"):
        if not callable(principal_resolver):
            raise ValueError("Trusted authentication resolver required")
        self.app, self.service, self.resolve, self.path = (
            app,
            service,
            principal_resolver,
            mcp_path.rstrip("/") or "/",
        )
        from mcp.server.transport_security import TransportSecurityMiddleware

        self.security = TransportSecurityMiddleware(transport_security)

    @property
    def router(self):
        app = self.app
        while not hasattr(app, "router") and hasattr(app, "app"):
            app = app.app
        return app.router

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or (scope.get("path", "").rstrip("/") or "/") != self.path:
            return await self.app(scope, receive, send)
        owner = await self.resolve(scope)
        if not isinstance(owner, EventPrincipal):
            return await JSONResponse({"error": "Authentication required"}, status_code=401)(
                scope, receive, send
            )
        from starlette.requests import HTTPConnection
        from telegram_mcp.transport.protocol import (
            MCP_PROTOCOL_VERSION_HEADER,
            UnsupportedProtocolVersionError,
            negotiate_protocol_version,
        )

        connection = HTTPConnection(scope)
        rejected = await self.security.validate_request(
            connection, is_post=scope.get("method") == "POST"
        )
        if rejected is not None:
            return await rejected(scope, receive, send)
        try:
            negotiate_protocol_version(connection.headers.get(MCP_PROTOCOL_VERSION_HEADER))
        except UnsupportedProtocolVersionError as exc:
            return await JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": str(exc)}},
                status_code=400,
            )(scope, receive, send)
        if scope.get("method") != "POST":
            return await self.app(scope, receive, send)
        chunks = []
        size = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                return
            size += len(message.get("body", b""))
            if size > 262144:
                return await JSONResponse({"error": "Request too large"}, status_code=413)(
                    scope, receive, send
                )
            chunks.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        body = b"".join(chunks)
        try:
            rpc = json.loads(body)
        except ValueError:
            rpc = {}
        if isinstance(rpc, dict) and rpc.get("method") in (
            "events/list",
            "events/subscribe",
            "events/unsubscribe",
        ):
            response = {"jsonrpc": "2.0", "id": rpc.get("id")}
            try:
                response["result"] = await self.service.dispatch(
                    owner, rpc["method"], rpc.get("params") or {}
                )
            except (ProtocolError, ValueError) as exc:
                response["error"] = {"code": getattr(exc, "code", -32602), "message": str(exc)}
                if getattr(exc, "reason", None):
                    response["error"]["data"] = {"reason": exc.reason}
            return await JSONResponse(response)(scope, receive, send)
        delivered = False

        async def replay_receive():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        if isinstance(rpc, dict) and rpc.get("method") == "initialize":
            # SDK JSON and SSE responses are both retained, changing only capability data.
            output = []

            async def capture(message):
                output.append(message)

            await self.app(scope, replay_receive, capture)
            raw = b"".join(m.get("body", b"") for m in output if m["type"] == "http.response.body")

            def augment(data):
                if isinstance(data, dict) and isinstance(data.get("result"), dict):
                    data["result"].setdefault("capabilities", {})["events"] = {}
                return data

            try:
                raw = canonical(augment(json.loads(raw))).encode()
            except ValueError:
                lines = raw.decode().splitlines(keepends=True)
                raw = "".join(
                    (
                        "data: " + canonical(augment(json.loads(line[5:].strip()))) + "\n"
                        if line.startswith("data:")
                        else line
                    )
                    for line in lines
                ).encode()
            for message in output:
                if message["type"] == "http.response.start":
                    message["headers"] = [
                        (k, v) for k, v in message["headers"] if k.lower() != b"content-length"
                    ]
                    await send(message)
            return await send({"type": "http.response.body", "body": raw})
        return await self.app(scope, replay_receive, send)
