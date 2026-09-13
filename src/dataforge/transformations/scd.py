"""Slowly Changing Dimensions (SCD Type 1 and Type 2).

SCD Type 1 - overwrite
    The dimension keeps only the current value. History is lost. Cheap and
    simple; use when history doesn't matter (e.g. correcting a typo).

SCD Type 2 - versioned history
    Each change creates a new versioned row with validity bounds
    (``effective_from`` / ``effective_to``) and an ``is_current`` flag plus
    a surrogate key. Use when you must answer "what was the value *as of* a
    point in time".

Both functions are pure and operate on pandas DataFrames so they are
easy to unit test.
"""

from __future__ import annotations

import hashlib

import pandas as pd

from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.transformations.scd")

_HIGH_DATE = pd.Timestamp("9999-12-31", tz="UTC")


def _surrogate_key(natural_key: str, effective_from: str) -> str:
    """Deterministic surrogate key from natural key + effective date."""
    return hashlib.sha1(f"{natural_key}|{effective_from}".encode()).hexdigest()[:16]


def scd_type_1(
    current: pd.DataFrame,
    updates: pd.DataFrame,
    *,
    key: str,
    tracked_columns: list[str],
) -> pd.DataFrame:
    """Apply SCD Type 1 (overwrite) updates onto the current dimension.

    Existing rows have their ``tracked_columns`` overwritten by the update;
    new keys are appended. No history is retained.
    """
    merged = current.set_index(key)
    upd = updates.set_index(key)
    for col in tracked_columns:
        if col in upd.columns:
            merged.loc[upd.index.intersection(merged.index), col] = upd[col]
    new_keys = upd.index.difference(merged.index)
    if len(new_keys):
        merged = pd.concat([merged, upd.loc[new_keys, merged.columns.intersection(upd.columns)]])
    result = merged.reset_index()
    _log.info("scd type 1 applied", extra={"rows": len(result), "new_keys": len(new_keys)})
    return result


def scd_type_2(
    current_dim: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    key: str,
    tracked_columns: list[str],
    effective_col: str = "updated_at",
) -> pd.DataFrame:
    """Merge ``incoming`` source rows into a Type 2 dimension.

    Behaviour:
    - **new record**     : key not seen before -> insert as current version.
    - **changed record** : tracked column(s) differ from current version ->
                           expire the old version (set ``effective_to`` +
                           ``is_current=False``) and insert a new current
                           version starting at the change's ``updated_at``.
    - **unchanged**      : no tracked column changed -> no new version.
    """
    incoming = incoming.copy()
    incoming[effective_col] = pd.to_datetime(incoming[effective_col], errors="coerce", utc=True)
    incoming = incoming.sort_values(effective_col).drop_duplicates(key, keep="last")

    meta_cols = ["surrogate_key", "effective_from", "effective_to", "is_current"]
    business_cols = [
        c for c in incoming.columns if c in (set(tracked_columns) | {key} | {effective_col})
    ]

    if current_dim is None or current_dim.empty:
        rows = []
        for _, r in incoming.iterrows():
            eff_from = (
                r[effective_col] if pd.notna(r[effective_col]) else pd.Timestamp.now(tz="UTC")
            )
            rec = {c: r[c] for c in business_cols}
            rec["surrogate_key"] = _surrogate_key(str(r[key]), str(eff_from))
            rec["effective_from"] = eff_from
            rec["effective_to"] = _HIGH_DATE
            rec["is_current"] = True
            rows.append(rec)
        result = pd.DataFrame(rows)
        _log.info("scd type 2 initial load", extra={"rows": len(result)})
        return result

    dim = current_dim.copy()
    dim["effective_from"] = pd.to_datetime(dim["effective_from"], errors="coerce", utc=True)
    dim["effective_to"] = pd.to_datetime(dim["effective_to"], errors="coerce", utc=True)

    new_versions = []
    expired = 0
    inserted = 0

    for _, r in incoming.iterrows():
        k = r[key]
        eff_from = r[effective_col] if pd.notna(r[effective_col]) else pd.Timestamp.now(tz="UTC")
        current_mask = (dim[key] == k) & (dim["is_current"])

        if not current_mask.any():
            rec = {c: r[c] for c in business_cols}
            rec["surrogate_key"] = _surrogate_key(str(k), str(eff_from))
            rec["effective_from"] = eff_from
            rec["effective_to"] = _HIGH_DATE
            rec["is_current"] = True
            new_versions.append(rec)
            inserted += 1
            continue

        current_row = dim.loc[current_mask].iloc[0]
        changed = any(str(current_row.get(c)) != str(r.get(c)) for c in tracked_columns)
        if not changed:
            continue

        dim.loc[current_mask, "is_current"] = False
        dim.loc[current_mask, "effective_to"] = eff_from
        expired += 1

        rec = {c: r[c] for c in business_cols}
        rec["surrogate_key"] = _surrogate_key(str(k), str(eff_from))
        rec["effective_from"] = eff_from
        rec["effective_to"] = _HIGH_DATE
        rec["is_current"] = True
        new_versions.append(rec)
        inserted += 1

    result = pd.concat([dim, pd.DataFrame(new_versions)], ignore_index=True)
    ordered = business_cols + meta_cols
    result = result[[c for c in ordered if c in result.columns]]
    _log.info(
        "scd type 2 merge",
        extra={"expired": expired, "inserted": inserted, "total_rows": len(result)},
    )
    return result
