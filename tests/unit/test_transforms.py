"""Unit tests for pandas transformation logic (pure functions)."""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.transformations import pandas_transforms as P


@pytest.fixture()
def sample_txns() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["T1", "T2", "T2", "T3"],
            "account_id": ["A1", "A2", "A2", None],
            "amount": [100.0, -5.0, 50.0, 20.0],
            "currency": ["USD", "EUR", "EUR", "GBP"],
            "status": ["POSTED", "POSTED", "REVERSED", "POSTED"],
            "fraud_flag": [0, 0, 0, 1],
            "transaction_timestamp": pd.to_datetime(
                ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"], utc=True
            ),
            "updated_at": pd.to_datetime(
                ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04"], utc=True
            ),
        }
    )


@pytest.mark.unit
def test_filter_valid_transactions_drops_bad_rows(sample_txns):
    out = P.filter_valid_transactions(sample_txns)
    # Drops: negative amount (T2 row1), reversed (T2 row2), null account (T3).
    assert set(out["transaction_id"]) == {"T1"}


@pytest.mark.unit
def test_deduplicate_keeps_latest(sample_txns):
    out = P.deduplicate(sample_txns, ["transaction_id"], order_by="updated_at")
    assert out["transaction_id"].is_unique
    # T2 kept the later (REVERSED) row because it has the larger updated_at.
    t2 = out.loc[out["transaction_id"] == "T2"].iloc[0]
    assert t2["status"] == "REVERSED"


@pytest.mark.unit
def test_cast_types_coerces_bad_values():
    df = pd.DataFrame({"amount": ["10.5", "oops"], "transaction_timestamp": ["2025-01-01", "nope"]})
    out = P.cast_types(df, {"amount": "decimal", "transaction_timestamp": "timestamp"})
    assert out["amount"].tolist()[0] == 10.5
    assert pd.isna(out["amount"].tolist()[1])  # uncoercible -> NaN
    assert pd.isna(out["transaction_timestamp"].tolist()[1])


@pytest.mark.unit
def test_select_columns_missing_raises(sample_txns):
    with pytest.raises(KeyError):
        P.select_columns(sample_txns, ["transaction_id", "does_not_exist"])


@pytest.mark.unit
def test_anti_and_semi_join():
    left = pd.DataFrame({"account_id": ["A1", "A2", "A9"]})
    right = pd.DataFrame({"account_id": ["A1", "A2"]})
    anti = P.anti_join(left, right, "account_id")
    semi = P.semi_join(left, right, "account_id")
    assert anti["account_id"].tolist() == ["A9"]
    assert set(semi["account_id"]) == {"A1", "A2"}


@pytest.mark.unit
def test_aggregate_daily_measures(sample_txns):
    daily = P.aggregate_daily_transactions(sample_txns)
    assert {"txn_count", "total_amount", "avg_amount", "fraud_count"} <= set(daily.columns)
    assert daily["txn_count"].sum() == len(sample_txns)
