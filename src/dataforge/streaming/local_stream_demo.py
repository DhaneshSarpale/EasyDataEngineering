"""End-to-end local streaming + fraud detection demo.

Run:  ``make stream-demo``  or
      ``python -m dataforge.streaming.local_stream_demo``

Produces a synthetic event stream (with embedded fraud scenarios) to an
in-memory Kinesis-like stream, consumes it with idempotency + checkpointing,
and prints the fraud alerts it detected.
"""

from __future__ import annotations

from collections import Counter

from dataforge.common.config import load_config
from dataforge.common.logging_utils import get_logger
from dataforge.streaming.consumer import StreamConsumer
from dataforge.streaming.events import generate_event_stream
from dataforge.streaming.local_stream import LocalStream
from dataforge.streaming.producer import produce

_log = get_logger("dataforge.streaming.demo")


def run() -> dict:
    config = load_config()
    stream = LocalStream(
        name=config.get("streaming.stream_name", "dataforge-dev-transactions"),
        shard_count=int(config.get("streaming.shard_count", 2)),
    )

    produced = produce(generate_event_stream(n=150, inject_fraud=True), stream)
    result = StreamConsumer(config).consume(stream)

    by_rule = Counter(a.rule for a in result.alerts)
    summary = {
        "produced": produced,
        "processed": result.processed,
        "duplicates_dropped": result.duplicates,
        "late_events": result.late,
        "alerts": len(result.alerts),
        "alerts_by_rule": dict(by_rule),
    }
    return summary


def main() -> int:
    summary = run()
    print("\nStreaming + fraud detection demo:")
    for k, v in summary.items():
        print(f"  {k:22s} {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
