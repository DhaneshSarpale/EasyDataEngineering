"""Core transformation patterns implemented with pandas.

These are the LOCAL MODE / test-friendly equivalents of the Spark
transforms. Each function is small, pure (no I/O), and typed so it can be
unit tested directly. The docstrings map each function to the classic
transformation vocabulary (filter, select, rename, cast, null handling,
deduplication, joins, aggregation).
"""

from __future__ import annotations

import pandas as pd

from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.transformations.pandas")


# ----------------------------------------------------------------------
# Filtering
# ----------------------------------------------------------------------
def filter_valid_transactions(df: pd.DataFrame, *, min_amount: float = 0.0) -> pd.DataFrame:
    """Keep only rows that pass basic validity filters.

    Filters out: null account_id, non-positive amount, and reversed rows.
    """
    mask = df["account_id"].notna() & (df["amount"] > min_amount) & (df["status"] != "REVERSED")
    return df.loc[mask].copy()


# ----------------------------------------------------------------------
# Column selection / renaming
# ----------------------------------------------------------------------
def select_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Project a subset of columns (column pruning)."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(f"select_columns: missing columns {missing}")
    return df[columns].copy()


def rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Standardise column names (e.g. snake_case conforming)."""
    return df.rename(columns=mapping).copy()


# ----------------------------------------------------------------------
# Type casting
# ----------------------------------------------------------------------
def cast_types(df: pd.DataFrame, casts: dict[str, str]) -> pd.DataFrame:
    """Cast columns to target types.

    Supported target strings: ``date``, ``timestamp``, ``decimal``/``float``,
    ``int``, ``string``. Uncoercible values become NaT/NaN (surfaced later
    by data-quality checks rather than crashing the transform).
    """
    out = df.copy()
    for col, target in casts.items():
        if col not in out.columns:
            continue
        if target in ("timestamp", "datetime"):
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
        elif target == "date":
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.date
        elif target in ("decimal", "float"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
        elif target == "int":
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
        elif target == "string":
            out[col] = out[col].astype("string")
    return out


# ----------------------------------------------------------------------
# Null handling
# ----------------------------------------------------------------------
def drop_null_rows(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Drop rows with nulls in any of ``subset`` (required-field enforcement)."""
    return df.dropna(subset=subset).copy()


def fill_nulls(df: pd.DataFrame, defaults: dict[str, object]) -> pd.DataFrame:
    """Fill nulls with per-column default values."""
    return df.fillna(value=defaults).copy()


def conditional_default(
    df: pd.DataFrame, column: str, condition_col: str, when_value: object, default: object
) -> pd.DataFrame:
    """Set ``column`` to ``default`` where ``condition_col == when_value``."""
    out = df.copy()
    out.loc[out[condition_col] == when_value, column] = default
    return out


# ----------------------------------------------------------------------
# Deduplication
# ----------------------------------------------------------------------
def deduplicate(
    df: pd.DataFrame,
    keys: list[str],
    *,
    order_by: str | None = None,
    keep: str = "last",
) -> pd.DataFrame:
    """Remove duplicate rows on ``keys``, keeping the latest by ``order_by``.

    Pandas equivalent of the SQL
    ``ROW_NUMBER() OVER (PARTITION BY keys ORDER BY order_by DESC) = 1`` idiom.
    """
    out = df.copy()
    if order_by:
        out = out.sort_values(order_by)
    before = len(out)
    out = out.drop_duplicates(subset=keys, keep=keep).reset_index(drop=True)
    _log.info(
        "deduplicated",
        extra={"keys": keys, "removed": before - len(out), "remaining": len(out)},
    )
    return out


# ----------------------------------------------------------------------
# Joins
# ----------------------------------------------------------------------
def join_transactions_customers(
    transactions: pd.DataFrame,
    accounts: pd.DataFrame,
    customers: pd.DataFrame,
    *,
    how: str = "inner",
) -> pd.DataFrame:
    """Enrich transactions with customer attributes via accounts.

    transactions -> accounts (account_id) -> customers (customer_id).
    ``how`` demonstrates inner/left/right/outer join semantics.
    """
    enriched = transactions.merge(
        accounts[["account_id", "customer_id", "account_type"]],
        on="account_id",
        how=how,
    )
    enriched = enriched.merge(
        customers[["customer_id", "segment", "country"]].rename(
            columns={"country": "customer_country"}
        ),
        on="customer_id",
        how=how,
    )
    return enriched


def anti_join(left: pd.DataFrame, right: pd.DataFrame, key: str) -> pd.DataFrame:
    """LEFT ANTI JOIN: rows in ``left`` whose ``key`` is absent from ``right``.

    Used e.g. to find transactions referencing a non-existent account.
    """
    merged = left.merge(right[[key]].drop_duplicates(), on=key, how="left", indicator=True)
    return merged.loc[merged["_merge"] == "left_only"].drop(columns="_merge").copy()


def semi_join(left: pd.DataFrame, right: pd.DataFrame, key: str) -> pd.DataFrame:
    """LEFT SEMI JOIN: rows in ``left`` whose ``key`` exists in ``right``."""
    keys = right[key].drop_duplicates()
    return left.loc[left[key].isin(keys)].copy()


# ----------------------------------------------------------------------
# Aggregations
# ----------------------------------------------------------------------
def aggregate_daily_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Build the daily_transaction_summary gold table.

    Grain: one row per (transaction_date, currency). Demonstrates
    COUNT / SUM / AVG / MIN / MAX / COUNT DISTINCT.
    """
    out = df.copy()
    out["transaction_date"] = pd.to_datetime(
        out["transaction_timestamp"], errors="coerce", utc=True
    ).dt.date
    grouped = (
        out.groupby(["transaction_date", "currency"], dropna=True)
        .agg(
            txn_count=("transaction_id", "count"),
            distinct_accounts=("account_id", "nunique"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
            min_amount=("amount", "min"),
            max_amount=("amount", "max"),
            fraud_count=("fraud_flag", "sum"),
        )
        .reset_index()
    )
    grouped["avg_amount"] = grouped["avg_amount"].round(2)
    return grouped
