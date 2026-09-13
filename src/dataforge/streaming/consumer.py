"""Streaming consumer with idempotency, checkpointing, and fraud scoring.

For each record the consumer:

1. **de-duplicates** using the transaction_id (at-least-once -> once).
2. **checkpoints** the last processed sequence number per shard.
3. **scores** the event with the :class:`FraudDetector` and collects alerts.
4. **routes** clean events onward (here: returns them; in AWS a sink would
   write to S3 / OpenSearch / Redshift via Firehose).

Late-arriving events are counted and still scored, but they do not advance
the fraud detector's travel baseline (handled inside the detector).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger
from dataforge.streaming.fraud import FraudAlert, FraudDetector
from dataforge.streaming.local_stream import LocalStream

_log = get_logger("dataforge.streaming.consumer")


@dataclass
class ConsumeResult:
    """Summary of a consumption run."""

    processed: int = 0
    duplicates: int = 0
    late: int = 0
    alerts: list[FraudAlert] = field(default_factory=list)


class StreamConsumer:
    """Consume a local stream, applying idempotency + fraud detection."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or load_config()
        self.detector = FraudDetector(self.config)
        self._seen_ids: set[str] = set()
        self._checkpoints: dict[int, int] = {}
        self._last_ts_by_customer: dict[str, str] = {}

    def consume(self, stream: LocalStream) -> ConsumeResult:
        """Process every record in the stream once."""
        result = ConsumeResult()
        for shard in range(stream.shard_count):
            for record in stream.read_shard(shard):
                data = record["data"]
                txn_id = data["transaction_id"]

                # 1) idempotency
                if txn_id in self._seen_ids:
                    result.duplicates += 1
                    continue
                self._seen_ids.add(txn_id)

                # detect late arrival (per customer: ts older than that
                # customer's latest seen event)
                ts = data["timestamp"]
                cust = data["customer_id"]
                last_ts = self._last_ts_by_customer.get(cust)
                if last_ts is not None and ts < last_ts:
                    result.late += 1
                else:
                    self._last_ts_by_customer[cust] = ts

                # 3) fraud scoring
                from dataforge.streaming.events import TransactionEvent

                event = TransactionEvent(**data)
                result.alerts.extend(self.detector.evaluate(event))

                result.processed += 1
                # 2) checkpoint
                self._checkpoints[shard] = record["sequence_number"]

        _log.info(
            "consume complete",
            extra={
                "processed": result.processed,
                "duplicates": result.duplicates,
                "late": result.late,
                "alerts": len(result.alerts),
            },
        )
        return result
