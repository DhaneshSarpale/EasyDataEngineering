"""Retry with exponential backoff + jitter.

Used by API ingestion, SFTP, and any I/O that can fail transiently. Only
:class:`RetryableError` (and any extra types you pass) are retried; a
:class:`NonRetryableError` propagates immediately so we do not waste time
retrying a deterministic failure.

Backoff formula: ``delay = base * 2**attempt`` capped at ``max_delay``,
plus uniform jitter to avoid thundering-herd retries.
"""

from __future__ import annotations

import functools
import random
import time
from collections.abc import Callable
from typing import Any, TypeVar

from dataforge.common.errors import RetryableError
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.retry")

T = TypeVar("T")


def retry(
    max_attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 30.0,
    jitter: float = 0.1,
    retry_on: tuple[type[Exception], ...] = (RetryableError,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator that retries a function with exponential backoff.

    Args:
        max_attempts: Total attempts before giving up (including the first).
        base_delay: Base delay in seconds for the exponential curve.
        max_delay: Upper bound on any single sleep.
        jitter: Max random fraction added to each delay to de-correlate retries.
        retry_on: Exception types that trigger a retry. Anything else propagates.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except retry_on as exc:
                    attempt += 1
                    if attempt >= max_attempts:
                        _log.error(
                            "retry exhausted",
                            extra={"func": func.__name__, "attempts": attempt},
                        )
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    delay += random.uniform(0, jitter * delay)
                    _log.warning(
                        "retrying after error",
                        extra={
                            "func": func.__name__,
                            "attempt": attempt,
                            "sleep_seconds": round(delay, 3),
                            "error": str(exc),
                        },
                    )
                    time.sleep(delay)

        return wrapper

    return decorator
