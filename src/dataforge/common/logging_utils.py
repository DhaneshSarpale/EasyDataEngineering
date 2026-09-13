"""Structured JSON logging.

Data pipelines are observed through their logs. Emitting structured JSON
(rather than free-form strings) means CloudWatch Logs Insights / Athena /
OpenSearch can query fields like ``run_id``, ``records_failed`` directly.

Usage::

    log = get_logger("transaction_pipeline", run_id="abc123")
    log.info("extract complete", extra={"records_read": 10000})

In AWS, Lambda/Glue stdout is captured into CloudWatch Logs automatically,
so writing JSON to stdout is sufficient - no extra SDK calls required.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

_RESERVED = set(logging.makeLogRecord({}).__dict__.keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON objects."""

    def __init__(self, static_fields: dict[str, Any] | None = None) -> None:
        super().__init__()
        self._static = static_fields or {}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(self._static)
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(
    name: str,
    *,
    level: str = "INFO",
    json_output: bool = True,
    **static_fields: Any,
) -> logging.Logger:
    """Return a configured logger.

    Args:
        name: Logger name, typically the pipeline / module name.
        level: Log level string (``INFO``, ``DEBUG`` ...).
        json_output: Emit JSON when True, human-readable text otherwise.
        **static_fields: Fields attached to every record (e.g. ``run_id``).
    """
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    if json_output:
        handler.setFormatter(JsonFormatter(static_fields=static_fields))
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s"))
    logger.addHandler(handler)
    return logger
