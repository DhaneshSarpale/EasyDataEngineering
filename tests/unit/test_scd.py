"""Unit tests for Slowly Changing Dimensions (Type 1 & Type 2)."""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.transformations.scd import scd_type_1, scd_type_2


@pytest.fixture()
def base_customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": ["C1", "C2", "C3"],
            "city": ["London", "Paris", "Tokyo"],
            "country": ["GB", "FR", "JP"],
            "updated_at": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-01"], utc=True),
        }
    )


@pytest.mark.unit
def test_scd_type_1_overwrites_and_appends(base_customers):
    updates = pd.DataFrame(
        {
            "customer_id": ["C1", "C9"],
            "city": ["Berlin", "Madrid"],
            "country": ["DE", "ES"],
            "updated_at": pd.to_datetime(["2025-02-01", "2025-02-01"], utc=True),
        }
    )
    out = scd_type_1(
        base_customers, updates, key="customer_id", tracked_columns=["city", "country"]
    )
    c1 = out.loc[out["customer_id"] == "C1"].iloc[0]
    assert c1["city"] == "Berlin"  # overwritten
    assert "C9" in set(out["customer_id"])  # new key appended
    assert len(out) == 4


@pytest.mark.unit
def test_scd_type_2_initial_load_all_current(base_customers):
    dim = scd_type_2(None, base_customers, key="customer_id", tracked_columns=["city", "country"])
    assert len(dim) == 3
    assert dim["is_current"].all()
    assert (dim["effective_to"] == pd.Timestamp("9999-12-31", tz="UTC")).all()
    assert dim["surrogate_key"].is_unique


@pytest.mark.unit
def test_scd_type_2_change_creates_version(base_customers):
    dim0 = scd_type_2(None, base_customers, key="customer_id", tracked_columns=["city", "country"])
    change = pd.DataFrame(
        {
            "customer_id": ["C1"],
            "city": ["Berlin"],
            "country": ["DE"],
            "updated_at": pd.to_datetime(["2025-06-01"], utc=True),
        }
    )
    dim1 = scd_type_2(dim0, change, key="customer_id", tracked_columns=["city", "country"])
    # One expired + one new version for C1 -> 4 rows total, 3 current.
    assert len(dim1) == 4
    assert int(dim1["is_current"].sum()) == 3
    c1_current = dim1[(dim1["customer_id"] == "C1") & (dim1["is_current"])].iloc[0]
    assert c1_current["city"] == "Berlin"
    c1_expired = dim1[(dim1["customer_id"] == "C1") & (~dim1["is_current"])].iloc[0]
    assert c1_expired["city"] == "London"


@pytest.mark.unit
def test_scd_type_2_unchanged_is_noop(base_customers):
    dim0 = scd_type_2(None, base_customers, key="customer_id", tracked_columns=["city", "country"])
    dim1 = scd_type_2(dim0, base_customers, key="customer_id", tracked_columns=["city", "country"])
    assert len(dim1) == len(dim0)  # no new versions
