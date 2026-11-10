"""Exercise signed requests through the production HTTP parser and pinned backend."""

import asyncio
import base64
import hashlib
import hmac
import json
import ssl
from unittest.mock import AsyncMock

import httpcore
import pytest

from telegram_mcp.event_bridge.callbacks import (
    CallbackClient,
    CallbackError,
    decode_secret,
    encode_body,
    signed_headers,
    validate_callback_url,
)

SECRET = "whsec_" + base64.b64encode(b"a" * 32).decode()
OTHER_SECRET = "whsec_" + base64.b64encode(b"b" * 32).decode()
PUBLIC = "93.184.216.34"


def response(status=200, body=b"{}", headers=b""):
    return (
        f"HTTP/1.1 {status} Response\r\nContent-Length: {len(body)}\r\n".encode()
        + headers
        + b"\r\n"
        + body
    )


class Wire(httpcore.AsyncNetworkStream):
    def __init__(self, callback):
        self.callback = callback
        self.request = bytearray()
        self.remaining = None
        self.tls = None
        self.closed = False

    async def read(self, max_bytes, timeout=None):
        if self.remaining is None:
            self.remaining = self.callback(bytes(self.request))
        result, self.remaining = self.remaining[:max_bytes], self.remaining[max_bytes:]
        return result

    async def write(self, buffer, timeout=None):
        self.request.extend(buffer)

    async def aclose(self):
        self.closed = True

    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        assert ssl_context.check_hostname
        assert ssl_context.verify_mode == ssl.CERT_REQUIRED
        self.tls = server_hostname
        return self


class Network(httpcore.AsyncNetworkBackend):
    def __init__(self, callback=lambda _: response()):
        self.callback = callback
        self.connections = []

    async def connect_tcp(self, host, port, **kwargs):
        wire = Wire(self.callback)
        self.connections.append((host, port, wire))
        return wire


def request_parts(raw):
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    headers = dict(line.split(b": ", 1) for line in lines[1:])
    return lines[0], headers, body


def test_signatures_cover_exact_serialized_bytes_and_rotation():
    body = encode_body({"text": "Привет", "eventId": "event"})
    headers = signed_headers("event", "sub", body, SECRET, OTHER_SECRET, timestamp=123)
    assert json.loads(body)["text"] == "Привет"
    assert headers["webhook-id"] == "event"
    assert headers["X-MCP-Subscription-Id"] == "sub"
    expected = []
    for key in (b"a" * 32, b"b" * 32):
        signature = hmac.new(key, b"event.123." + body, hashlib.sha256).digest()
        expected.append("v1," + base64.b64encode(signature).decode())
    assert headers["webhook-signature"].split() == expected
    assert decode_secret(SECRET) == b"a" * 32
    for value in ("not-a-secret", "whsec_!!!!", "whsec_" + base64.b64encode(b"x").decode()):
        with pytest.raises(ValueError):
            decode_secret(value)
    with pytest.raises(ValueError):
        encode_body({"text": "x" * 262144})


@pytest.mark.asyncio
async def test_post_pins_ip_preserves_original_tls_host_and_exact_request():
    network = Network(lambda _: response(202, b"accepted", b"Retry-After: 5\r\n"))
    resolver = AsyncMock(return_value=(PUBLIC,))
    client = CallbackClient(resolver=resolver, network_backend=network)
    body = encode_body({"eventId": "event", "data": {"message_id": 7}})
    headers = signed_headers("event", "sub", body, SECRET, timestamp=123)
    result = await client.post(
        "https://callback.example:8443/events?token=public", body, headers, capture_body=True
    )
    assert result.status == 202 and result.body == b"accepted"
    assert result.headers["retry-after"] == "5"
    host, port, wire = network.connections[0]
    assert (host, port, wire.tls) == (PUBLIC, 8443, "callback.example")
    first, received_headers, received_body = request_parts(bytes(wire.request))
    assert first == b"POST /events?token=public HTTP/1.1"
    assert received_headers[b"Host"] == b"callback.example:8443"
    assert received_body == body
    assert received_headers[b"webhook-signature"].decode() == headers["webhook-signature"]
    assert wire.closed


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "192.0.0.8",
        "192.88.99.1",
        "::1",
        "fe80::1",
        "::ffff:127.0.0.1",
        "64:ff9b::7f00:1",
        "2002:7f00:1::",
    ],
)
@pytest.mark.asyncio
async def test_any_nonpublic_dns_answer_blocks_entire_destination(address):
    network = Network()
    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC, address)), network_backend=network
    )
    with pytest.raises(CallbackError):
        await client.post("https://callback.example/events", b"{}", {}, capture_body=True)
    assert network.connections == []


@pytest.mark.asyncio
async def test_rebinding_is_checked_again_on_next_post():
    resolver = AsyncMock(side_effect=[(PUBLIC,), ("192.168.1.1",)])
    network = Network()
    client = CallbackClient(resolver=resolver, network_backend=network)
    await client.post("https://callback.example/events", b"{}", {})
    with pytest.raises(CallbackError):
        await client.post("https://callback.example/events", b"{}", {})
    assert len(network.connections) == 1


@pytest.mark.asyncio
async def test_redirect_is_returned_without_following_it():
    network = Network(lambda _: response(302, headers=b"Location: https://127.0.0.1/private\r\n"))
    client = CallbackClient(resolver=AsyncMock(return_value=(PUBLIC,)), network_backend=network)
    result = await client.post("https://callback.example/events", b"{}", {})
    assert result.status == 302
    assert len(network.connections) == 1


@pytest.mark.asyncio
async def test_verification_is_signed_and_cached_for_owner_url_only():
    challenges = []
    clock = [0.0]

    def receiver(raw):
        _, headers, body = request_parts(raw)
        envelope = json.loads(body)
        assert envelope["type"] == "verification"
        challenges.append(envelope["challenge"])
        assert headers[b"webhook-id"].startswith(b"msg_verification_")
        prefix = headers[b"webhook-id"] + b"." + headers[b"webhook-timestamp"] + b"."
        expected = base64.b64encode(hmac.new(b"a" * 32, prefix + body, hashlib.sha256).digest())
        assert headers[b"webhook-signature"] == b"v1," + expected
        return response(body=json.dumps({"challenge": envelope["challenge"]}).encode())

    network = Network(receiver)
    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC,)),
        network_backend=network,
        clock=lambda: clock[0],
    )
    await client.verify("owner", "https://callback.example/events", SECRET, "sub")
    await client.verify("owner", "https://callback.example/events", SECRET, "another-sub")
    assert len(challenges) == 1
    await client.verify("another-owner", "https://callback.example/events", SECRET, "sub")
    assert len(challenges) == 2 and challenges[0] != challenges[1]
    clock[0] = 301
    await client.verify("owner", "https://callback.example/events", SECRET, "sub")
    assert len(set(challenges)) == 3


@pytest.mark.asyncio
async def test_wrong_challenge_and_host_flood_limit_never_verify():
    network = Network(lambda _: response(body=b'{"challenge":"wrong"}'))
    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC,)),
        network_backend=network,
        host_verifications_per_minute=1,
    )
    for path in ("one", "two"):
        with pytest.raises(CallbackError) as error:
            await client.verify("owner", f"https://callback.example/{path}", SECRET, "sub")
        assert error.value.reason == "challenge_failed"
        assert "wrong" not in str(error.value)
    assert len(network.connections) == 1


@pytest.mark.asyncio
async def test_oversized_response_is_bounded_and_not_returned():
    network = Network(lambda _: response(body=b"x" * 20000))
    client = CallbackClient(resolver=AsyncMock(return_value=(PUBLIC,)), network_backend=network)
    with pytest.raises(CallbackError):
        await client.post("https://callback.example/events", b"{}", {}, capture_body=True)
    assert network.connections[0][2].closed


@pytest.mark.asyncio
async def test_delivery_acknowledgement_does_not_require_response_body():
    network = Network(lambda _: response(202, body=b"x" * 20000))
    client = CallbackClient(resolver=AsyncMock(return_value=(PUBLIC,)), network_backend=network)
    result = await client.post("https://callback.example/events", b"{}", {})
    assert result.status == 202
    assert result.body == b""
    assert network.connections[0][2].closed


@pytest.mark.parametrize(
    "url",
    [
        "http://callback.example",
        "https://user:password@callback.example/",
        "https://callback.example/#fragment",
        "https://callback.example/\r\nHeader:x",
        "https://callback.example:0/",
        "https://[fe80::1%25en0]/",
    ],
)
def test_unsafe_url_forms_are_rejected(url):
    with pytest.raises(ValueError):
        validate_callback_url(url)


@pytest.mark.asyncio
async def test_verification_expires_and_rejects_expired_challenge():
    clock = [0.0]

    def receiver(raw):
        _, _, body = request_parts(raw)
        clock[0] += 31
        return response(body=encode_body({"challenge": json.loads(body)["challenge"]}))

    network = Network(receiver)
    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC,)),
        network_backend=network,
        clock=lambda: clock[0],
    )
    with pytest.raises(CallbackError) as error:
        await client.verify("owner", "https://callback.example/events", SECRET, "sub")
    assert error.value.reason == "challenge_failed"


@pytest.mark.asyncio
async def test_timeout_and_tls_failures_expose_only_safe_categories():
    class SlowNetwork(Network):
        async def connect_tcp(self, host, port, **kwargs):
            await asyncio.sleep(1)

    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC,)),
        network_backend=SlowNetwork(),
        timeout=0.01,
    )
    with pytest.raises(CallbackError) as error:
        await client.post("https://callback.example/events", b"{}", {})
    assert error.value.reason == "timeout"

    class TLSFailure(Network):
        async def connect_tcp(self, host, port, **kwargs):
            raise ssl.SSLError("arbitrary endpoint secret")

    client = CallbackClient(
        resolver=AsyncMock(return_value=(PUBLIC,)), network_backend=TLSFailure()
    )
    with pytest.raises(CallbackError) as error:
        await client.post("https://callback.example/events", b"{}", {})
    assert error.value.reason == "tls_error"
    assert str(error.value) == "tls_error"
