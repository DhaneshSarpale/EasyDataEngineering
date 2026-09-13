"""Real-time streaming: producer, consumer, fraud rules, local demo.

LOCAL MODE uses an in-memory queue that mimics a Kinesis Data Stream
(records, partition keys, at-least-once delivery, duplicates, late arrival).
AWS MODE swaps the local stream for boto3 Kinesis calls - see
``kinesis_reference.py`` and ``firehose_reference.py``.
"""

from dataforge.streaming.events import TransactionEvent, generate_event_stream
from dataforge.streaming.fraud import FraudDetector

__all__ = ["TransactionEvent", "generate_event_stream", "FraudDetector"]
