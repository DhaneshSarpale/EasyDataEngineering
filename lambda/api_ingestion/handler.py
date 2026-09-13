"""Lambda: scheduled REST API ingestion (FX rates).

Invoked on a schedule (EventBridge). Pulls incremental FX rates from the
API using the resilient :class:`ApiClient` (pagination + retry + backoff)
and writes the raw response to S3. Lightweight and bursty -> a good Lambda
fit.
"""

from __future__ import annotations

import json
import os
from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    from dataforge.ingestion.api.client import ApiClient

    updated_since = event.get("updated_since")
    client = ApiClient()
    rates = client.get_all_rates(updated_since=updated_since)

    key = _write_raw(rates)
    return {"statusCode": 200, "records": len(rates), "s3_key": key}


def _write_raw(rates: list[dict[str, Any]]) -> str:
    """Write the raw API payload to S3 (or stdout locally)."""
    bucket = os.environ.get("RAW_BUCKET")
    key = "api/fx_rates/latest.json"
    body = json.dumps({"data": rates})
    if not bucket:
        print(json.dumps({"level": "INFO", "message": "no RAW_BUCKET; local no-op",
                          "records": len(rates)}))
        return key
    try:
        import boto3

        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode())
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"level": "ERROR", "message": str(exc)}))
    return key
