"""A hand-rolled HTTP endpoint exposing broker metrics as JSON, and the
static dashboard that polls it.

No web framework here on purpose: this project's whole point is
implementing the real mechanics rather than wrapping something that does
it already, and an endpoint that only ever needs to answer "GET /metrics"
and "GET /" doesn't need routing, middleware, or anything else a real
framework provides. This is NOT a general-purpose HTTP server -- no
keep-alive, no chunked encoding, no request bodies, nothing beyond a bare
GET. That's an intentional scope limit, not an oversight.

build_snapshot() is a pure function over TopicRegistry's own bookkeeping,
with no dependency on sockets or HTTP, so it can be (and is) unit tested
without opening a connection -- the same isolation pattern used
throughout this project.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from broker.topics import TopicRegistry

DASHBOARD_HTML_PATH = Path(__file__).resolve().parent.parent / "dashboard" / "index.html"


def build_snapshot(registry: TopicRegistry) -> dict:
    topics = {}
    for topic in sorted(registry.known_topics()):
        topics[topic] = {
            "messages_published": registry.publish_count(topic),
            "broadcast_subscribers": registry.subscriber_count(topic),
            "groups": registry.groups_for_topic(topic),
        }
    return {
        "connected_clients": registry.active_connections,
        "pending_acks": registry.ack_tracker.pending_count() if registry.ack_tracker is not None else 0,
        "topics": topics,
    }


def _http_response(status: str, content_type: str, body: bytes) -> bytes:
    headers = (
        f"HTTP/1.1 {status}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    return headers + body


async def _handle_http_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, registry: TopicRegistry
) -> None:
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5)
        # Headers aren't needed for either endpoint, but they still have to
        # be read off the stream (up to the blank line that ends them) so
        # the connection isn't left in a confusing half-read state.
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if line in (b"\r\n", b""):
                break

        parts = request_line.decode("latin-1", errors="replace").split()
        path = parts[1] if len(parts) >= 2 else "/"

        if path == "/metrics":
            body = json.dumps(build_snapshot(registry), indent=2).encode("utf-8")
            writer.write(_http_response("200 OK", "application/json", body))
        elif path == "/":
            body = DASHBOARD_HTML_PATH.read_bytes()
            writer.write(_http_response("200 OK", "text/html; charset=utf-8", body))
        else:
            body = b"not found"
            writer.write(_http_response("404 Not Found", "text/plain", body))
        await writer.drain()
    except (asyncio.TimeoutError, ConnectionError):
        pass
    finally:
        writer.close()
        await writer.wait_closed()


async def run_metrics_server(
    registry: TopicRegistry, host: str = "127.0.0.1", port: int = 8766
) -> asyncio.AbstractServer:
    async def _handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await _handle_http_connection(reader, writer, registry)

    server = await asyncio.start_server(_handler, host, port)
    return server
