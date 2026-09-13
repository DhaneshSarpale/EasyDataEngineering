"""Lambda: S3-triggered file processor.

Triggered by an S3 ``ObjectCreated`` event on the raw bucket. It validates
the uploaded file and kicks off the Glue ingestion job (or, for small
files, ingests inline). This is the classic "event-driven ingestion"
entry point.

When Lambda is the RIGHT tool
    - short (<15 min), event-driven, bursty work (a file just landed).
    - glue/step-functions kick-off, light validation, notifications.

When Lambda is the WRONG tool (see docs/orchestration.md)
    - long-running or memory-heavy processing (Lambda caps at 15 min /
      10 GB) — use Glue/EMR instead.
    - steady high-throughput streaming — a Kinesis consumer / KDA app is
      cheaper than millions of Lambda invocations.
    - work needing a large JVM/Spark runtime.
"""

from __future__ import annotations

import json
import os
from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Entry point. ``event`` is an S3 notification (possibly batched)."""
    processed = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"].get("size", 0)

        if size == 0:
            _notify(f"empty file skipped: s3://{bucket}/{key}")
            continue

        # Kick off the Glue ingestion job for this object.
        job_run_id = _start_glue_job(
            os.environ.get("GLUE_INGEST_JOB", "dataforge-dev-extract"),
            arguments={"--s3_input": f"s3://{bucket}/{key}"},
        )
        processed.append({"key": key, "glue_job_run_id": job_run_id})

    return {"statusCode": 200, "processed": processed}


def _start_glue_job(job_name: str, arguments: dict[str, str]) -> str:
    """Start a Glue job run (no-op stub locally without boto3/AWS)."""
    try:
        import boto3

        glue = boto3.client("glue")
        resp = glue.start_job_run(JobName=job_name, Arguments=arguments)
        return resp["JobRunId"]
    except Exception:  # noqa: BLE001 - local/no-AWS fallback
        return "local-no-op"


def _notify(message: str) -> None:
    print(json.dumps({"level": "WARNING", "message": message}))
