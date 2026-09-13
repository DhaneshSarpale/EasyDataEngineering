"""Secret resolution - never hardcode credentials.

Resolution order:

1. Environment variable (LOCAL MODE, CI).
2. AWS Secrets Manager (AWS MODE), looked up lazily so ``boto3`` is only
   imported when actually needed.

This keeps secrets out of source, config files, and logs. See
``docs/security.md`` for the full rationale and IAM policy examples.
"""

from __future__ import annotations

import json
import os
from typing import Any

from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.secrets")


def get_secret(
    env_var: str, *, aws_secret_name: str | None = None, region: str = "eu-west-1"
) -> str:
    """Resolve a single secret string.

    Args:
        env_var: Environment variable checked first (LOCAL MODE).
        aws_secret_name: Secrets Manager secret id used as a fallback.
        region: AWS region for Secrets Manager.

    Raises:
        KeyError: if the secret cannot be resolved from either source.
    """
    if env_var in os.environ:
        return os.environ[env_var]

    if aws_secret_name:
        value = _get_aws_secret(aws_secret_name, region)
        if isinstance(value, str):
            return value

    raise KeyError(
        f"Secret not found. Set env var '{env_var}' " f"or provide AWS secret '{aws_secret_name}'."
    )


def get_secret_dict(aws_secret_name: str, region: str = "eu-west-1") -> dict[str, Any]:
    """Fetch a JSON secret (e.g. DB connection blob) from Secrets Manager."""
    value = _get_aws_secret(aws_secret_name, region)
    if isinstance(value, dict):
        return value
    return json.loads(value)


def _get_aws_secret(secret_name: str, region: str) -> Any:
    try:
        import boto3  # imported lazily; not needed in LOCAL MODE
    except ImportError as exc:  # pragma: no cover - depends on optional dep
        raise KeyError("boto3 is not installed; cannot read AWS secrets in LOCAL MODE") from exc

    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    payload = resp.get("SecretString", "{}")
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload
