"""Streaming producer.

LOCAL MODE writes events to a :class:`LocalStream`; AWS MODE would call
``kinesis.put_records`` (batched). The producer uses ``customer_id`` as the
partition key so a customer's events stay ordered on one shard.
"""

from __future__ import annotations

from collections.abc import Iterable

from dataforge.common.logging_utils import get_logger
from dataforge.streaming.events import TransactionEvent
from dataforge.streaming.local_stream import LocalStream

_log = get_logger("dataforge.streaming.producer")


def produce(events: Iterable[TransactionEvent], stream: LocalStream) -> int:
    """Publish events to the stream; returns the count produced."""
    count = 0
    for event in events:
        stream.put_record(event.to_dict(), partition_key=event.partition_key)
        count += 1
    _log.info("produced events", extra={"stream": stream.name, "count": count})
    return count
