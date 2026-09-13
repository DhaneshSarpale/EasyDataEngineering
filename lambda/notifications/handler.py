"""Lambda: publish pipeline notifications to SNS.

Called from the Step Functions failure path (or on success for a heads-up).
Formats a concise, structured message and publishes it to the alerts topic.
"""

from __future__ import annotations

import json
import os
from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    status = event.get("status", "FAILED")
    pipeline = event.get("pipeline", "unknown")
    detail = event.get("error") or event.get("detail") or {}

    message = {
        "pipeline": pipeline,
        "status": status,
        "detail": detail,
    }
    _publish(subject=f"DataForge {pipeline} {status}", message=json.dumps(message))
    return {"published": True, "message": message}


def _publish(subject: str, message: str) -> None:
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    if not topic_arn:
        print(json.dumps({"level": "INFO", "message": "no SNS_TOPIC_ARN; local no-op",
                          "subject": subject}))
        return
    try:
        import boto3

        boto3.client("sns").publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"level": "ERROR", "message": str(exc)}))
