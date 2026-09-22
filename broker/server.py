"""Milestone 1: TCP connection handling and frame I/O.

This server accepts connections and can read/write frames correctly over a
real socket. It does not yet understand CONNECT/PUBLISH/SUBSCRIBE -- that
command dispatch is Milestone 2. For now, each received frame is logged and
echoed back unchanged. The echo exists purely so Milestone 1 can be proven
correct end-to-end over a real socket (see tests/test_server.py); it will be
replaced by real command dispatch in Milestone 2, not kept alongside it.
"""

from __future__ import annotations

import asyncio
import logging

from broker.protocol import ConnectionClosedError, encode_frame, read_frame

logger = logging.getLogger("broker.server")


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    logger.info("connection opened: %s", peer)
    try:
        while True:
            frame = await read_frame(reader)
            logger.info("received frame from %s: %s", peer, frame)
            writer.write(encode_frame(frame))
            await writer.drain()
    except ConnectionClosedError:
        logger.info("connection closed: %s", peer)
    finally:
        writer.close()
        await writer.wait_closed()


async def run_server(host: str = "127.0.0.1", port: int = 8765) -> asyncio.AbstractServer:
    server = await asyncio.start_server(handle_connection, host, port)
    addr = server.sockets[0].getsockname()
    logger.info("broker listening on %s:%s", addr[0], addr[1])
    return server


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    server = await run_server()
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
