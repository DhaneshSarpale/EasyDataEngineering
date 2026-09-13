"""Unit tests for idempotency ledger, checksums, and retry/backoff."""

from __future__ import annotations

import pytest

from dataforge.common.errors import NonRetryableError, RetryableError
from dataforge.common.idempotency import IdempotencyStore, file_checksum
from dataforge.common.retry import retry


@pytest.mark.unit
def test_file_checksum_stable_and_content_sensitive(tmp_path):
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    a.write_text("hello")
    b.write_text("hello")
    assert file_checksum(a) == file_checksum(b)  # same content -> same hash
    b.write_text("world")
    assert file_checksum(a) != file_checksum(b)


@pytest.mark.unit
def test_event_idempotency(tmp_path):
    store = IdempotencyStore(tmp_path / "idem.sqlite")
    assert store.mark_event("E1") is True  # first claim
    assert store.mark_event("E1") is False  # duplicate dropped
    assert store.is_event_processed("E1")


@pytest.mark.unit
def test_file_idempotency(tmp_path):
    store = IdempotencyStore(tmp_path / "idem.sqlite")
    f = tmp_path / "data.csv"
    f.write_text("x\n1\n")
    checksum = file_checksum(f)
    assert not store.is_file_processed(f.name, checksum)
    store.mark_file(f.name, checksum, status="PROCESSED")
    assert store.is_file_processed(f.name, checksum)


@pytest.mark.unit
def test_retry_retries_then_succeeds():
    calls = {"n": 0}

    @retry(max_attempts=4, base_delay=0.0)
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["n"] == 3


@pytest.mark.unit
def test_retry_does_not_retry_non_retryable():
    calls = {"n": 0}

    @retry(max_attempts=4, base_delay=0.0)
    def boom():
        calls["n"] += 1
        raise NonRetryableError("deterministic")

    with pytest.raises(NonRetryableError):
        boom()
    assert calls["n"] == 1  # no retries
