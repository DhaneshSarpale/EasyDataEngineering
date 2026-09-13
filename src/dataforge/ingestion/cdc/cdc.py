"""Change Data Capture: events, storage, and latest-state reconstruction.

CDC captures row-level INSERT / UPDATE / DELETE from a source database's
transaction log and streams them downstream, so the lake reflects changes
without re-reading whole tables.

Concepts
--------
- **Full load**        one-time snapshot of the current table state.
- **CDC**              the continuous stream of changes after the snapshot.
- **Full load + CDC**  snapshot first, then apply the change stream on top
                       (AWS DMS "migration type: full-load-and-cdc").
- **Log-based CDC**    changes read from the DB's write-ahead log (Postgres
                       logical replication / MySQL binlog). Low source
                       impact and captures every change, including deletes.

Each CDC event mirrors the DMS envelope:

    {
      "operation": "INSERT|UPDATE|DELETE",
      "before_image": {...} | null,
      "after_image":  {...} | null,
      "timestamp": "<iso>",
      "primary_key": "<pk value>"
    }

Reconstructing latest state = for each primary key, take the most recent
event; if it's a DELETE the row is absent, otherwise the row equals its
``after_image``. This is the same "apply the log" logic a MERGE performs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.ingestion.cdc")


def generate_cdc_events(
    snapshot: pd.DataFrame,
    primary_key: str,
    *,
    n_updates: int = 20,
    n_inserts: int = 10,
    n_deletes: int = 5,
    seed: int = 7,
) -> pd.DataFrame:
    """Synthesise a CDC event stream from a snapshot DataFrame."""
    rng = np.random.default_rng(seed)
    base_ts = datetime(2025, 6, 1, tzinfo=timezone.utc)
    events: list[dict[str, Any]] = []
    seq = 0

    def _ts() -> str:
        nonlocal seq
        seq += 1
        return (base_ts + timedelta(minutes=seq)).isoformat()

    keys = snapshot[primary_key].tolist()

    mutable_col = next(
        (c for c in ("city", "status", "balance", "amount") if c in snapshot.columns),
        None,
    )
    for key in rng.choice(keys, size=min(n_updates, len(keys)), replace=False):
        before = snapshot.loc[snapshot[primary_key] == key].iloc[0].to_dict()
        after = dict(before)
        if mutable_col:
            after[mutable_col] = f"UPDATED_{seq}"
        events.append(
            {
                "operation": "UPDATE",
                "primary_key": key,
                "before_image": before,
                "after_image": after,
                "timestamp": _ts(),
            }
        )

    template = snapshot.iloc[0].to_dict()
    for i in range(n_inserts):
        new_key = f"CDCNEW{i:04d}"
        after = dict(template)
        after[primary_key] = new_key
        events.append(
            {
                "operation": "INSERT",
                "primary_key": new_key,
                "before_image": None,
                "after_image": after,
                "timestamp": _ts(),
            }
        )

    for key in rng.choice(keys, size=min(n_deletes, len(keys)), replace=False):
        before = snapshot.loc[snapshot[primary_key] == key].iloc[0].to_dict()
        events.append(
            {
                "operation": "DELETE",
                "primary_key": key,
                "before_image": before,
                "after_image": None,
                "timestamp": _ts(),
            }
        )

    df = pd.DataFrame(events)
    _log.info(
        "cdc events generated",
        extra={
            "inserts": n_inserts,
            "updates": min(n_updates, len(keys)),
            "deletes": min(n_deletes, len(keys)),
        },
    )
    return df


def reconstruct_latest_state(
    snapshot: pd.DataFrame,
    cdc_events: pd.DataFrame,
    primary_key: str,
) -> pd.DataFrame:
    """Apply a CDC stream on top of a full-load snapshot (full-load + CDC).

    Algorithm:
    1. Start from the snapshot keyed by ``primary_key``.
    2. Process events in timestamp order.
    3. INSERT/UPDATE -> upsert the ``after_image``.
    4. DELETE        -> drop the key.
    """
    state: dict[Any, dict[str, Any]] = {
        row[primary_key]: row.to_dict() for _, row in snapshot.iterrows()
    }

    ordered = cdc_events.sort_values("timestamp")
    for _, event in ordered.iterrows():
        op = event["operation"]
        key = event["primary_key"]
        if op in ("INSERT", "UPDATE"):
            state[key] = dict(event["after_image"])
        elif op == "DELETE":
            state.pop(key, None)

    result = pd.DataFrame(list(state.values()))
    _log.info(
        "latest state reconstructed",
        extra={"snapshot_rows": len(snapshot), "final_rows": len(result)},
    )
    return result.reset_index(drop=True)
