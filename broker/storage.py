"""Persistent per-topic append-only log.

Each topic's messages live in their own newline-delimited JSON (JSONL) file
under a data directory. One line per message, written in publish order.

Why JSONL instead of SQLite for this milestone: the access pattern is
purely sequential appends plus full sequential reads (no random access, no
queries, no joins), which is exactly what a flat append-only file is good
at. SQLite would add a real dependency and transactional machinery this
project doesn't need yet. If a later milestone needs random access (e.g.
"resume from message N" without scanning from the start), that's a
concrete, documented reason to reconsider -- not a decision made
speculatively now.
"""

from __future__ import annotations

import json
from pathlib import Path


class TopicLog:
    """One append-only log file for one topic."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: dict) -> None:
        # Opened and flushed per write rather than kept open: this trades
        # some throughput for a simple correctness guarantee (every
        # append() call either lands on disk before returning, or raises).
        # If Milestone 7's throughput measurement shows this is a real
        # bottleneck, batching writes is a documented option -- not
        # something to guess at optimizing before it's measured.
        with self._path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()

    def read_all(self) -> list[dict]:
        if not self._path.exists():
            return []
        records = []
        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records


class LogStore:
    """Owns one TopicLog per topic, all rooted under a single data directory."""

    def __init__(self, data_dir: str | Path) -> None:
        self._data_dir = Path(data_dir)
        self._logs: dict[str, TopicLog] = {}

    def _log_for(self, topic: str) -> TopicLog:
        if topic not in self._logs:
            self._logs[topic] = TopicLog(self._data_dir / f"{_safe_filename(topic)}.jsonl")
        return self._logs[topic]

    def append(self, topic: str, record: dict) -> None:
        self._log_for(topic).append(record)

    def read_all(self, topic: str) -> list[dict]:
        return self._log_for(topic).read_all()

    def max_message_id_seen(self) -> int:
        """Scan every existing topic log and return the highest numeric
        suffix seen in any "m-<n>" message_id, or 0 if there are none.

        Called once at startup so a fresh TopicRegistry can resume message
        id assignment after whatever was already written, instead of
        restarting at m-1 and colliding with ids already on disk.
        """
        if not self._data_dir.exists():
            return 0
        max_id = 0
        for path in self._data_dir.glob("*.jsonl"):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    message_id = json.loads(line).get("message_id", "")
                    if message_id.startswith("m-"):
                        try:
                            max_id = max(max_id, int(message_id[2:]))
                        except ValueError:
                            continue
        return max_id


def _safe_filename(topic: str) -> str:
    """Map a topic name to a safe filename component.

    A topic name is client-supplied. Without this, a topic named something
    like "../../etc/passwd" would let a client make the broker read or
    write outside the data directory.
    """
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in topic)
