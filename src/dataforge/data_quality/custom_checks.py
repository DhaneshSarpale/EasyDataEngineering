"""Custom Python data-quality checks beyond the declarative engine.

Some checks are easier to express as plain functions - profiling,
cross-dataset referential integrity, and freshness. These return simple
result dicts that can be logged or merged into the DQ report.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def profile_column(df: pd.DataFrame, column: str) -> dict[str, Any]:
    """Basic column profile: null %, distinct count, min/max where numeric."""
    series = df[column]
    profile: dict[str, Any] = {
        "column": column,
        "rows": len(series),
        "nulls": int(series.isna().sum()),
        "null_pct": round(100.0 * series.isna().mean(), 3),
        "distinct": int(series.nunique(dropna=True)),
    }
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        profile.update(
            {
                "min": float(numeric.min()),
                "max": float(numeric.max()),
                "mean": round(float(numeric.mean()), 3),
            }
        )
    return profile


def referential_integrity(
    child: pd.DataFrame, parent: pd.DataFrame, *, child_key: str, parent_key: str
) -> dict[str, Any]:
    """Count child rows whose foreign key is absent from the parent table."""
    valid = set(parent[parent_key].dropna().unique())
    orphaned = child.loc[~child[child_key].isin(valid) & child[child_key].notna()]
    return {
        "check": "referential_integrity",
        "child_key": child_key,
        "parent_key": parent_key,
        "orphaned_rows": int(len(orphaned)),
        "passed": len(orphaned) == 0,
    }


def freshness(df: pd.DataFrame, timestamp_col: str, max_age_days: int = 2) -> dict[str, Any]:
    """Assert the newest record isn't older than ``max_age_days``.

    Note: on the synthetic historical dataset this will typically "fail"
    (data is from 2024-2025); it's included to demonstrate the check shape
    used for real freshness SLAs.
    """
    ts = pd.to_datetime(df[timestamp_col], errors="coerce", utc=True)
    latest = ts.max()
    age_days = (pd.Timestamp.now(tz="UTC") - latest).days if pd.notna(latest) else None
    return {
        "check": "freshness",
        "timestamp_col": timestamp_col,
        "latest": str(latest),
        "age_days": age_days,
        "max_age_days": max_age_days,
        "passed": age_days is not None and age_days <= max_age_days,
    }
