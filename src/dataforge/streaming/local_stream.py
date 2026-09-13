"""An in-memory stream that mimics Kinesis semantics for LOCAL MODE.

Provides just enough of the Kinesis mental model to demonstrate the
streaming concepts without an AWS account:

- **shards**          records are hashed by partition key across N shards,
                      preserving per-key ordering within a shard.
- **put_record**      producer API.
- **read_shard**      consumer API (returns records in arrival order).
- **at-least-once**   the demo can re-deliver a record to exercise the
                      consumer's idempotency handling.

The public method names mirror the boto3 Kinesis client so swapping to the
real service (``kinesis_reference.py``) is a small change.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any


class LocalStream:
    """A minimal, ordered, sharded in-memory stream."""

    def __init__(self, name: str, shard_count: int = 2) -> None:
        self.name = name
        self.shard_count = shard_count
        self._shards: dict[int, list[dict[str, Any]]] = defaultdict(list)

    def _shard_for(self, partition_key: str) -> int:
        digest = hashlib.md5(partition_key.encode()).hexdigest()
        return int(digest, 16) % self.shard_count

    def put_record(self, data: dict[str, Any], partition_key: str) -> dict[str, Any]:
        """Append a record to the shard selected by ``partition_key``."""
        shard = self._shard_for(partition_key)
        seq = len(self._shards[shard])
        record = {"data": data, "partition_key": partition_key, "sequence_number": seq}
        self._shards[shard].append(record)
        return {"ShardId": f"shard-{shard}", "SequenceNumber": str(seq)}

    def read_shard(self, shard_id: int) -> list[dict[str, Any]]:
        """Return all records currently in a shard (in order)."""
        return list(self._shards.get(shard_id, []))

    def read_all(self) -> list[dict[str, Any]]:
        """Return every record across shards, ordered per-shard.

        Global ordering across shards is not guaranteed (matching Kinesis);
        per-partition-key ordering is, because a key always maps to one shard.
        """
        out: list[dict[str, Any]] = []
        for shard in range(self.shard_count):
            out.extend(self._shards.get(shard, []))
        return out
