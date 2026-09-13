"""Build a Kimball-style star schema (dimensions + facts) from silver data.

Concepts
--------
- **Natural key**   business identifier from the source (e.g. customer_id).
- **Surrogate key** system-generated key for the dimension row. Insulates
                    facts from source-key changes and enables SCD2 versioning.
- **Fact grain**    the level of detail of a fact row. ``fact_transaction``
                    grain = one row per transaction.
- **Dimension grain** one row per dimension entity (or per version for SCD2).
"""

from __future__ import annotations

import hashlib

import pandas as pd


def _sk(prefix: str, natural_key: str) -> str:
    """Deterministic surrogate key: prefix + short hash of the natural key."""
    h = hashlib.sha1(str(natural_key).encode()).hexdigest()[:12]
    return f"{prefix}_{h}"


def build_dim_customer(customers: pd.DataFrame) -> pd.DataFrame:
    dim = customers.copy()
    dim["customer_sk"] = dim["customer_id"].map(lambda k: _sk("CUSTSK", k))
    cols = [
        "customer_sk",
        "customer_id",
        "first_name",
        "last_name",
        "segment",
        "city",
        "country",
    ]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_account(accounts: pd.DataFrame) -> pd.DataFrame:
    dim = accounts.copy()
    dim["account_sk"] = dim["account_id"].map(lambda k: _sk("ACCSK", k))
    cols = ["account_sk", "account_id", "customer_id", "account_type", "currency", "status"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_card(cards: pd.DataFrame) -> pd.DataFrame:
    dim = cards.copy()
    dim["card_sk"] = dim["card_id"].map(lambda k: _sk("CARDSK", k))
    cols = ["card_sk", "card_id", "account_id", "card_type", "status"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_merchant(merchants: pd.DataFrame) -> pd.DataFrame:
    dim = merchants.copy()
    dim["merchant_sk"] = dim["merchant_id"].map(lambda k: _sk("MERSK", k))
    cols = ["merchant_sk", "merchant_id", "merchant_name", "category", "country"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_branch(branches: pd.DataFrame) -> pd.DataFrame:
    dim = branches.copy()
    dim["branch_sk"] = dim["branch_id"].map(lambda k: _sk("BRSK", k))
    cols = ["branch_sk", "branch_id", "branch_name", "city", "country"]
    return dim[[c for c in cols if c in dim.columns]]


def build_dim_date(transactions: pd.DataFrame) -> pd.DataFrame:
    """Conformed date dimension derived from the transaction date range."""
    ts = pd.to_datetime(transactions["transaction_timestamp"], errors="coerce", utc=True)
    ts = ts.dropna()
    if ts.empty:
        return pd.DataFrame(
            columns=["date_sk", "date", "year", "quarter", "month", "day", "day_of_week"]
        )
    dates = pd.date_range(ts.min().normalize(), ts.max().normalize(), freq="D")
    dim = pd.DataFrame({"date": dates})
    dim["date_sk"] = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["year"] = dim["date"].dt.year
    dim["quarter"] = dim["date"].dt.quarter
    dim["month"] = dim["date"].dt.month
    dim["day"] = dim["date"].dt.day
    dim["day_of_week"] = dim["date"].dt.day_name()
    return dim[["date_sk", "date", "year", "quarter", "month", "day", "day_of_week"]]


def build_fact_transaction(
    transactions: pd.DataFrame,
    dim_account: pd.DataFrame,
    dim_merchant: pd.DataFrame,
) -> pd.DataFrame:
    """Build fact_transaction at grain = one row per transaction.

    Facts store surrogate-key foreign keys (not natural keys) plus the
    numeric *measures* (amount, fraud_flag).
    """
    fact = transactions.copy()
    fact["date_sk"] = pd.to_datetime(
        fact["transaction_timestamp"], errors="coerce", utc=True
    ).dt.strftime("%Y%m%d")
    fact = fact.merge(dim_account[["account_id", "account_sk"]], on="account_id", how="left")
    fact = fact.merge(dim_merchant[["merchant_id", "merchant_sk"]], on="merchant_id", how="left")
    cols = [
        "transaction_id",
        "account_sk",
        "merchant_sk",
        "date_sk",
        "transaction_type",
        "amount",
        "currency",
        "channel",
        "status",
        "fraud_flag",
    ]
    return fact[[c for c in cols if c in fact.columns]]
