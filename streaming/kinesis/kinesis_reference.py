"""AWS-MODE reference: Kinesis Data Streams producer & consumer with boto3.

Not run in LOCAL MODE. Shows how the local-stream methods map onto real
Kinesis calls. Concepts demonstrated:

- **partition keys / shards / ordering**: records with the same partition
  key land on the same shard, preserving order for that key.
- **batching**: ``put_records`` sends up to 500 records per call.
- **checkpointing**: consumers track a shard iterator / sequence number.
  Production consumers use the Kinesis Client Library (KCL) which
  checkpoints to DynamoDB and handles shard leases/resharding for you.
- **retries**: ``put_records`` can partially fail; failed records must be
  resent (they carry an ``ErrorCode``).
"""

from __future__ import annotations

PRODUCER = r'''
import json
import boto3
from dataforge.common.retry import retry
from dataforge.common.errors import RetryableError

kinesis = boto3.client("kinesis", region_name="eu-west-1")

@retry(max_attempts=5, base_delay=0.5)
def put_batch(stream_name: str, events: list[dict]) -> None:
    records = [
        {"Data": json.dumps(e), "PartitionKey": e["customer_id"]}
        for e in events
    ]
    resp = kinesis.put_records(StreamName=stream_name, Records=records)
    if resp["FailedRecordCount"]:
        # Resend only the failed records (partial failure handling).
        failed = [
            records[i] for i, r in enumerate(resp["Records"]) if "ErrorCode" in r
        ]
        raise RetryableError(f"{len(failed)} records failed; retrying")
'''

CONSUMER = r'''
import json
import boto3

kinesis = boto3.client("kinesis", region_name="eu-west-1")

def consume(stream_name: str):
    # A single-shard example; production uses the KCL for lease + checkpoint.
    shards = kinesis.list_shards(StreamName=stream_name)["Shards"]
    for shard in shards:
        it = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        while it:
            resp = kinesis.get_records(ShardIterator=it, Limit=500)
            for rec in resp["Records"]:
                event = json.loads(rec["Data"])
                yield event                    # -> idempotency + fraud scoring
            it = resp.get("NextShardIterator")
            if not resp["Records"]:
                break                          # caught up
'''
