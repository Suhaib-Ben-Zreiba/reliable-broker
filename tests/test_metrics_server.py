"""Integration tests for the metrics HTTP server: real sockets, a real
(if minimal) HTTP request/response, not mocked. No http.client or requests
library used for the test client either -- a hand-rolled request matches
the hand-rolled server, and avoids pulling in an HTTP client dependency
for a handful of tests."""

import asyncio

import pytest

from broker.metrics import run_metrics_server
from broker.topics import TopicRegistry


async def _http_get(addr, path: str) -> tuple[str, str, bytes]:
    """Returns (status_line, headers_text, body_bytes)."""
    reader, writer = await asyncio.open_connection(addr[0], addr[1])
    writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii"))
    await writer.drain()

    status_line = (await reader.readline()).decode("ascii").strip()
    headers = {}
    while True:
        line = (await reader.readline()).decode("ascii")
        if line in ("\r\n", ""):
            break
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()

    content_length = int(headers.get("content-length", "0"))
    body = await reader.readexactly(content_length) if content_length else b""

    writer.close()
    await writer.wait_closed()
    return status_line, headers, body


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_valid_json():
    registry = TopicRegistry()
    queue: asyncio.Queue = asyncio.Queue()
    registry.subscribe("orders", queue)
    await registry.publish("orders", {"id": 1})

    server = await run_metrics_server(registry, host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        status, headers, body = await _http_get(addr, "/metrics")

        assert status == "HTTP/1.1 200 OK"
        assert headers["content-type"] == "application/json"

        import json
        data = json.loads(body)
        assert data["topics"]["orders"]["messages_published"] == 1
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_dashboard_route_serves_html():
    registry = TopicRegistry()
    server = await run_metrics_server(registry, host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        status, headers, body = await _http_get(addr, "/")

        assert status == "HTTP/1.1 200 OK"
        assert "text/html" in headers["content-type"]
        assert b"<html" in body.lower()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unknown_path_returns_404():
    registry = TopicRegistry()
    server = await run_metrics_server(registry, host="127.0.0.1", port=0)
    addr = server.sockets[0].getsockname()

    try:
        status, _headers, _body = await _http_get(addr, "/does-not-exist")

        assert status == "HTTP/1.1 404 Not Found"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_metrics_reflects_live_connection_count_from_the_real_broker():
    """Ties the metrics server to the actual TCP broker, not just a
    manually-constructed registry: a real subscriber connection should be
    visible in /metrics."""
    from broker.server import run_server
    from broker.protocol import encode_frame

    registry = TopicRegistry()
    broker_server = await run_server(host="127.0.0.1", port=0, registry=registry)
    broker_addr = broker_server.sockets[0].getsockname()
    metrics_server = await run_metrics_server(registry, host="127.0.0.1", port=0)
    metrics_addr = metrics_server.sockets[0].getsockname()

    try:
        reader, writer = await asyncio.open_connection(broker_addr[0], broker_addr[1])
        writer.write(encode_frame({"type": "SUBSCRIBE", "topic": "orders"}))
        await writer.drain()
        await asyncio.sleep(0.05)  # let the server register the connection

        _status, _headers, body = await _http_get(metrics_addr, "/metrics")
        import json
        data = json.loads(body)
        assert data["connected_clients"] == 1

        writer.close()
        await writer.wait_closed()
    finally:
        broker_server.close()
        await broker_server.wait_closed()
        metrics_server.close()
        await metrics_server.wait_closed()
