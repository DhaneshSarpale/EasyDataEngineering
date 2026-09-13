"""Concrete data-quality rule sets for the banking domain.

These encode the rules from the project spec:

- transaction_id must be unique          (table-level)
- amount >= 0                             (row-level)
- currency must be valid                  (row-level)
- transaction_timestamp cannot be future  (row-level)
- account_id must be present              (row-level)
- status must be valid                    (row-level)

Valid vocabularies come from config (``data_quality.valid_*``) so they are
tuned per environment without code changes.
"""

from __future__ import annotations

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.data_quality.engine import Expectation


def transaction_rules(config: Config | None = None) -> list[Expectation]:
    """Return the transaction data-quality expectations."""
    config = config or load_config()
    valid_currencies = set(config.get("data_quality.valid_currencies", []))
    valid_status = set(config.get("data_quality.valid_txn_status", []))
    now = pd.Timestamp.now(tz="UTC")

    return [
        Expectation(
            name="transaction_id_unique",
            column="transaction_id",
            table_check=lambda df: not df["transaction_id"].duplicated().any(),
        ),
        Expectation(
            name="account_id_not_null",
            column="account_id",
            row_check=lambda df: df["account_id"].notna(),
        ),
        Expectation(
            name="amount_non_negative",
            column="amount",
            row_check=lambda df: pd.to_numeric(df["amount"], errors="coerce") >= 0,
        ),
        Expectation(
            name="currency_valid",
            column="currency",
            row_check=lambda df: df["currency"].isin(valid_currencies),
        ),
        Expectation(
            name="timestamp_not_future",
            column="transaction_timestamp",
            row_check=lambda df: pd.to_datetime(
                df["transaction_timestamp"], errors="coerce", utc=True
            )
            <= now,
        ),
        Expectation(
            name="status_valid",
            column="status",
            row_check=lambda df: df["status"].isin(valid_status),
            severity="warn",
        ),
    ]


def customer_rules(config: Config | None = None) -> list[Expectation]:
    """Return the customer data-quality expectations."""
    config = config or load_config()
    return [
        Expectation(
            name="customer_id_unique",
            column="customer_id",
            table_check=lambda df: not df["customer_id"].duplicated().any(),
        ),
        Expectation(
            name="email_present",
            column="email",
            row_check=lambda df: df["email"].notna() & (df["email"].astype(str) != ""),
            severity="warn",
        ),
        Expectation(
            name="first_name_present",
            column="first_name",
            row_check=lambda df: df["first_name"].astype(str).str.len() > 0,
        ),
    ]


def referential_integrity_rule(df: pd.DataFrame, valid_account_ids: set[str]) -> Expectation:
    """Row-level rule: account_id must exist in the accounts dimension."""
    return Expectation(
        name="account_exists",
        column="account_id",
        row_check=lambda d: d["account_id"].isin(valid_account_ids),
    )
