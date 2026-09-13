"""Window-function transforms (pandas equivalents of SQL window functions).

Each function corresponds to an analytic SQL window:

- ``latest_customer_record``   ROW_NUMBER() -> keep newest per key
- ``rank_transactions``        RANK / DENSE_RANK by amount within account
- ``previous_transaction``     LAG - previous txn amount per account
- ``next_transaction``         LEAD - next txn amount per account
- ``running_balance``          SUM() OVER (... ROWS UNBOUNDED PRECEDING)
- ``rolling_7d_amount``        windowed SUM over a 7-day time window
- ``spending_percentile``      percentile rank of spend across customers
- ``first_last_value``         FIRST_VALUE / LAST_VALUE per partition

The SQL originals live in ``sql/analytics/window_functions.sql``.
"""

from __future__ import annotations

import pandas as pd


def latest_customer_record(df: pd.DataFrame, key: str = "customer_id") -> pd.DataFrame:
    """ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC) = 1."""
    out = df.copy()
    out["updated_at"] = pd.to_datetime(out["updated_at"], errors="coerce", utc=True)
    out["_rn"] = out.sort_values("updated_at", ascending=False).groupby(key).cumcount() + 1
    return out.loc[out["_rn"] == 1].drop(columns="_rn").reset_index(drop=True)


def rank_transactions(
    df: pd.DataFrame, partition: str = "account_id", order: str = "amount"
) -> pd.DataFrame:
    """Add RANK and DENSE_RANK of ``order`` (desc) within each partition."""
    out = df.copy()
    out["amount_rank"] = (
        out.groupby(partition)[order].rank(method="min", ascending=False).astype("Int64")
    )
    out["amount_dense_rank"] = (
        out.groupby(partition)[order].rank(method="dense", ascending=False).astype("Int64")
    )
    return out


def previous_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """LAG: previous transaction amount per account (chronological)."""
    out = df.copy()
    out["transaction_timestamp"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    )
    out = out.sort_values(["account_id", "transaction_timestamp"])
    out["prev_amount"] = out.groupby("account_id")["amount"].shift(1)
    return out


def next_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """LEAD: next transaction amount per account (chronological)."""
    out = df.copy()
    out["transaction_timestamp"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    )
    out = out.sort_values(["account_id", "transaction_timestamp"])
    out["next_amount"] = out.groupby("account_id")["amount"].shift(-1)
    return out


def running_balance(df: pd.DataFrame) -> pd.DataFrame:
    """SUM() OVER (PARTITION BY account_id ORDER BY ts ROWS UNBOUNDED PRECEDING)."""
    out = df.copy()
    out["transaction_timestamp"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    )
    out = out.sort_values(["account_id", "transaction_timestamp"])
    out["running_amount"] = out.groupby("account_id")["amount"].cumsum()
    return out


def rolling_7d_amount(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling 7-day SUM of amount per account (time-based window)."""
    out = df.copy()
    out["transaction_timestamp"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    )
    out = out.dropna(subset=["transaction_timestamp"]).sort_values(
        ["account_id", "transaction_timestamp"]
    )
    out = out.set_index("transaction_timestamp")
    out["rolling_7d_amount"] = (
        out.groupby("account_id")["amount"].rolling("7D").sum().reset_index(level=0, drop=True)
    )
    return out.reset_index()


def spending_percentile(df: pd.DataFrame) -> pd.DataFrame:
    """Percentile rank of total spend across customers (customer segmentation)."""
    spend = df.groupby("account_id")["amount"].sum().reset_index(name="total_spend")
    spend["spend_percentile"] = spend["total_spend"].rank(pct=True).round(4)
    return spend


def first_last_value(df: pd.DataFrame) -> pd.DataFrame:
    """FIRST_VALUE / LAST_VALUE of amount per account (chronological)."""
    out = df.copy()
    out["transaction_timestamp"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    )
    out = out.sort_values(["account_id", "transaction_timestamp"])
    grp = out.groupby("account_id")["amount"]
    out["first_amount"] = grp.transform("first")
    out["last_amount"] = grp.transform("last")
    return out
