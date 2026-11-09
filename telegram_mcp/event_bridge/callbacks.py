"""Signed webhooks over HTTPS with destination checks on every connection.

The HTTP parser is httpcore's. Its network backend connects to a checked IP;
httpcore retains the original origin for Host, TLS SNI and certificate validation.
Each POST owns a fresh pool, so no unchecked proxy or redirect path is available.
"""

import asyncio
import base64
from collections import OrderedDict, deque
from dataclasses import dataclass
import hashlib
import hmac
import ipaddress
import json
import secrets
import socket
import ssl
import time
from typing import Mapping
from urllib.parse import urlsplit, urlunsplit

import httpcore

MAX_BODY_BYTES = 262144
MAX_RESPONSE_BYTES = 16384
_BLOCKED_V4 = tuple(
    ipaddress.ip_network(value) for value in ("192.0.0.0/24", "192.88.99.0/24", "198.18.0.0/15")
)
_BLOCKED_V6 = tuple(
    ipaddress.ip_network(value)
    for value in ("64:ff9b::/96", "64:ff9b:1::/48", "2001::/23", "2002::/16")
)


class CallbackError(Exception):
    """A safe category only: never expose an endpoint response, URL or secret."""

    def __init__(self, reason):
        if reason not in (
            "connection_refused",
            "timeout",
            "tls_error",
            "http_4xx",
            "http_5xx",
            "challenge_failed",
        ):
            raise ValueError("Unknown callback failure category")
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class CallbackResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


def validate_callback_url(url):
    """Return a canonical ASCII HTTPS URL without credentials or a fragment."""
    if (
        not isinstance(url, str)
        or not 1 <= len(url) <= 4096
        or any(ord(char) <= 32 or ord(char) >= 127 for char in url)
        or "#" in url
        or "\\" in url
    ):
        raise ValueError("Invalid HTTPS callback URL")
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port
        if (
            parts.scheme != "https"
            or not host
            or parts.username is not None
            or parts.password is not None
            or "%" in host
            or port is not None
            and not 1 <= port <= 65535
        ):
            raise ValueError
        netloc = f"[{host}]" if ":" in host else host
        if port is not None and port != 443:
            netloc += f":{port}"
        return urlunsplit(("https", netloc, parts.path or "/", parts.query, ""))
    except ValueError:
        raise ValueError("Invalid HTTPS callback URL") from None


def decode_secret(secret):
    try:
        if (
            not isinstance(secret, str)
            or not secret.startswith("whsec_")
            or not 38 <= len(secret) <= 94
        ):
            raise ValueError
        value = base64.b64decode(secret[6:], validate=True)
        if not 24 <= len(value) <= 64:
            raise ValueError
        return value
    except (ValueError, TypeError):
        raise ValueError("Invalid webhook signing secret") from None


def encode_body(value):
    body = json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("Webhook body exceeds its size limit")
    return body


def signed_headers(
    event_id, subscription_id, body, secret, previous_secret=None, *, timestamp=None
):
    """Sign these exact bytes; callers reuse the event ID but re-sign each retry."""
    for value in (event_id, subscription_id):
        if (
            not isinstance(value, str)
            or not 1 <= len(value) <= 200
            or any(ord(char) < 33 or ord(char) > 126 for char in value)
        ):
            raise ValueError("A safe bounded webhook identifier is required")
    if not isinstance(body, bytes) or len(body) > MAX_BODY_BYTES:
        raise ValueError("A bounded serialized webhook body is required")
    timestamp = int(time.time()) if timestamp is None else timestamp
    if type(timestamp) is not int or timestamp < 0:
        raise ValueError("A Unix-second signing timestamp is required")
    prefix = f"{event_id}.{timestamp}.".encode("ascii")
    signatures = []
    for value in (secret,) if previous_secret is None else (secret, previous_secret):
        digest = hmac.new(decode_secret(value), prefix + body, hashlib.sha256).digest()
        signatures.append("v1," + base64.b64encode(digest).decode("ascii"))
    return {
        "Content-Type": "application/json",
        "webhook-id": event_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": " ".join(signatures),
        "X-MCP-Subscription-Id": subscription_id,
    }


def _public_address(value):
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return _public_address(str(address.ipv4_mapped))
    if (
        not address.is_global
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
        or address.is_loopback
        or address.is_link_local
    ):
        return False
    if isinstance(address, ipaddress.IPv4Address):
        if str(address) in ("192.0.0.9", "192.0.0.10"):
            return True
        return not any(address in network for network in _BLOCKED_V4)
    return not any(address in network for network in _BLOCKED_V6)


async def _resolve(host, port):
    records = await asyncio.get_running_loop().getaddrinfo(
        host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
    )
    return tuple(dict.fromkeys(record[4][0] for record in records))


class _PinnedBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, resolver, backend):
        self.resolver, self.backend = resolver, backend

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
        addresses = tuple(await self.resolver(host, port))
        if not addresses or any(not _public_address(value) for value in addresses):
            raise CallbackError("connection_refused")
        return await self.backend.connect_tcp(
            addresses[0],
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )


class CallbackClient:
    """Bounded POST/verification transport; retry and durable grants belong to the worker.

    Resolver/backend injection lets tests exercise the real HTTP and TLS-origin
    routing without opening external sockets. Neither injection changes policy.
    Verification cache is soft state; persisted subscriptions retain their own
    verified grant. A new instance re-verifies newly created subscriptions.
    """

    def __init__(
        self,
        *,
        resolver=None,
        network_backend=None,
        timeout=10.0,
        clock=time.monotonic,
        verification_ttl=300.0,
        max_verified=256,
        host_verifications_per_minute=6,
    ):
        if not 0 < timeout <= 10 or not 0 < verification_ttl <= 3600:
            raise ValueError("Bounded callback deadlines are required")
        if type(max_verified) is not int or not 1 <= max_verified <= 1000:
            raise ValueError("A bounded verification cache is required")
        if (
            type(host_verifications_per_minute) is not int
            or not 1 <= host_verifications_per_minute <= 60
        ):
            raise ValueError("A bounded verification rate is required")
        self._backend = _PinnedBackend(
            resolver or _resolve, network_backend or httpcore.AnyIOBackend()
        )
        self._ssl = ssl.create_default_context()
        self._timeout, self._clock = timeout, clock
        self._ttl, self._max_verified = verification_ttl, max_verified
        self._host_limit = host_verifications_per_minute
        self._verified = OrderedDict()
        self._host_attempts = {}
        self._global_attempts = deque()
        self._verifying = set()

    async def post(self, url, body, headers, *, capture_body=False):
        """POST once. Deliveries acknowledge headers; verification requests a bounded body."""
        url = validate_callback_url(url)
        if not isinstance(body, bytes) or len(body) > MAX_BODY_BYTES:
            raise ValueError("A bounded serialized webhook body is required")
        outgoing = {}
        for name, value in headers.items():
            if (
                not isinstance(name, str)
                or not isinstance(value, str)
                or not name
                or any(ord(char) <= 32 or ord(char) >= 127 or char == ":" for char in name)
                or any(ord(char) < 32 or ord(char) >= 127 for char in value)
                or name.lower() in ("host", "connection", "content-length", "transfer-encoding")
            ):
                raise ValueError("Invalid callback request headers")
            outgoing[name] = value
        outgoing["Connection"] = "close"
        try:
            return await asyncio.wait_for(
                self._post(url, body, outgoing, capture_body), timeout=self._timeout
            )
        except CallbackError:
            raise
        except (asyncio.TimeoutError, httpcore.TimeoutException):
            raise CallbackError("timeout") from None
        except ssl.SSLError:
            raise CallbackError("tls_error") from None
        except httpcore.ConnectError as exc:
            reason = (
                "tls_error" if isinstance(exc.__cause__, ssl.SSLError) else "connection_refused"
            )
            raise CallbackError(reason) from None
        except (OSError, httpcore.NetworkError, httpcore.ProtocolError):
            raise CallbackError("connection_refused") from None

    async def _post(self, url, body, headers, capture_body):
        async with httpcore.AsyncConnectionPool(
            ssl_context=self._ssl,
            network_backend=self._backend,
            max_connections=1,
            max_keepalive_connections=0,
            retries=0,
            http2=False,
        ) as pool:
            async with pool.stream(
                "POST",
                url,
                headers=headers,
                content=body,
                extensions={
                    "timeout": {
                        name: self._timeout for name in ("connect", "read", "write", "pool")
                    }
                },
            ) as result:
                chunks, size = [], 0
                if capture_body:
                    async for chunk in result.aiter_stream():
                        size += len(chunk)
                        if size > MAX_RESPONSE_BYTES:
                            raise CallbackError("challenge_failed")
                        chunks.append(chunk)
                return CallbackResponse(
                    result.status,
                    {
                        key.decode("latin-1").lower(): value.decode("latin-1")
                        for key, value in result.headers
                    },
                    b"".join(chunks),
                )

    def _permit_verification(self, host, now):
        while self._global_attempts and self._global_attempts[0] <= now - 60:
            self._global_attempts.popleft()
        for key in tuple(self._host_attempts):
            attempts = self._host_attempts[key]
            while attempts and attempts[0] <= now - 60:
                attempts.popleft()
            if not attempts:
                del self._host_attempts[key]
        attempts = self._host_attempts.get(host, ())
        if len(attempts) >= self._host_limit or len(self._global_attempts) >= 60:
            raise CallbackError("challenge_failed")
        self._host_attempts.setdefault(host, deque()).append(now)
        self._global_attempts.append(now)

    async def verify(self, owner_id, url, secret, subscription_id):
        """Prove endpoint intent for this owner+URL before granting a subscription."""
        url = validate_callback_url(url)
        decode_secret(secret)
        if not isinstance(owner_id, str) or not owner_id or len(owner_id) > 200:
            raise ValueError("A bounded authenticated owner is required")
        now, key = self._clock(), (owner_id, url)
        for cached in tuple(self._verified):
            if self._verified[cached] <= now:
                del self._verified[cached]
        if key in self._verified:
            return
        if key in self._verifying:
            raise CallbackError("challenge_failed")
        self._permit_verification(urlsplit(url).hostname, now)
        self._verifying.add(key)
        try:
            challenge = secrets.token_urlsafe(32)
            body = encode_body({"type": "verification", "challenge": challenge})
            headers = signed_headers(
                "msg_verification_" + secrets.token_hex(16), subscription_id, body, secret
            )
            result = await self.post(url, body, headers, capture_body=True)
            if not 200 <= result.status < 300:
                reason = (
                    "http_5xx"
                    if result.status >= 500
                    else "http_4xx" if result.status >= 400 else "challenge_failed"
                )
                raise CallbackError(reason)
            try:
                echo = json.loads(result.body).get("challenge")
            except (ValueError, AttributeError, RecursionError):
                echo = None
            if (
                self._clock() - now > 30
                or not isinstance(echo, str)
                or not hmac.compare_digest(
                    challenge.encode(), echo.encode("utf-8", errors="replace")
                )
            ):
                raise CallbackError("challenge_failed")
            self._verified[key] = self._clock() + self._ttl
            while len(self._verified) > self._max_verified:
                self._verified.popitem(last=False)
        finally:
            self._verifying.discard(key)
