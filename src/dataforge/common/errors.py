"""Exception hierarchy for DataForge.

Distinguishing *retryable* from *non-retryable* errors is central to
robust pipelines:

- **Retryable**: transient failures (network timeout, throttling, a
  temporarily-unavailable dependency). Safe to retry with backoff.
- **Non-retryable**: deterministic failures (bad schema, invalid config,
  a malformed record). Retrying will always fail, so fail fast and route
  to a dead-letter / quarantine path instead.

The :func:`dataforge.common.retry.retry` decorator only retries
:class:`RetryableError` (and a configurable set of exception types).
"""

from __future__ import annotations


class DataForgeError(Exception):
    """Base class for all DataForge-raised errors."""


class RetryableError(DataForgeError):
    """A transient error that may succeed on retry (e.g. network, throttling)."""


class NonRetryableError(DataForgeError):
    """A deterministic error that will not succeed on retry (e.g. bad input)."""


class ConfigError(NonRetryableError):
    """Configuration is missing or invalid."""


class SchemaValidationError(NonRetryableError):
    """Incoming data does not match the expected schema."""


class DataQualityError(DataForgeError):
    """One or more data-quality expectations failed.

    Whether this halts the pipeline depends on
    ``data_quality.fail_pipeline_on_error`` in the environment config.
    """

    def __init__(self, message: str, failures: list[dict] | None = None) -> None:
        super().__init__(message)
        self.failures = failures or []
