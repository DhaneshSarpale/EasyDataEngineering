"""Data-quality tests: the rule engine catches the injected bad records."""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.data_quality.engine import Expectation, run_expectations
from dataforge.data_quality.rules import transaction_rules


@pytest.fixture()
def dirty_txns() -> pd.DataFrame:
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        {
            "transaction_id": ["T1", "T2", "T2"],  # duplicate T2
            "account_id": ["A1", None, "A3"],  # null account
            "amount": [10.0, -5.0, 20.0],  # negative amount
            "currency": ["USD", "XXX", "EUR"],  # invalid currency
            "status": ["POSTED", "POSTED", "POSTED"],
            "transaction_timestamp": [
                now - pd.Timedelta(days=1),
                now - pd.Timedelta(days=1),
                now + pd.Timedelta(days=400),  # future timestamp
            ],
        }
    )


@pytest.mark.data_quality
def test_rules_detect_all_bad_categories(dirty_txns):
    results = run_expectations(dirty_txns, transaction_rules())
    failed = {c["name"]: c for c in results.checks if not c["passed"]}
    assert "transaction_id_unique" in failed
    assert "account_id_not_null" in failed
    assert "amount_non_negative" in failed
    assert "currency_valid" in failed
    assert "timestamp_not_future" in failed
    assert results.passed is False


@pytest.mark.data_quality
def test_row_failure_mask_and_reasons(dirty_txns):
    results = run_expectations(dirty_txns, transaction_rules())
    mask = results.row_failure_mask(dirty_txns)
    assert mask.any()  # some rows flagged
    reasons = results.reason_for_rows(dirty_txns)
    # The null-account row should mention the account rule.
    assert reasons.str.contains("account_id_not_null").any()


@pytest.mark.data_quality
def test_clean_data_passes():
    now = pd.Timestamp.now(tz="UTC")
    clean = pd.DataFrame(
        {
            "transaction_id": ["T1", "T2"],
            "account_id": ["A1", "A2"],
            "amount": [10.0, 20.0],
            "currency": ["USD", "EUR"],
            "status": ["POSTED", "POSTED"],
            "transaction_timestamp": [now - pd.Timedelta(days=2), now - pd.Timedelta(days=1)],
        }
    )
    results = run_expectations(clean, transaction_rules())
    assert results.passed is True


@pytest.mark.data_quality
def test_custom_expectation():
    df = pd.DataFrame({"amount": [1, 2, 3]})
    exp = Expectation(name="amount_positive", column="amount", row_check=lambda d: d["amount"] > 0)
    results = run_expectations(df, [exp])
    assert results.passed
