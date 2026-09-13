"""Incremental pipeline: watermark extract -> transform -> MERGE -> commit.

Demonstrates an exactly-once/idempotent incremental load:

1. Read the current watermark for ``transactions`` (``updated_at``).
2. Extract only rows changed since then.
3. Transform (cast + dedup).
4. UPSERT (MERGE) into a local SQLite "warehouse" fact table keyed by
   ``transaction_id`` - re-running with the same batch is a no-op.
5. Commit the watermark ONLY after the load succeeds.

Because the watermark advances only on success, a crash between steps 4
and 5 simply reprocesses the same window next run; the MERGE makes that
safe (no double counting). AWS MODE maps this to Redshift ``MERGE`` +
``MetadataStore``/DynamoDB (see sql/incremental/merge_transactions.sql).

Run:  ``make run-incremental``
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger
from dataforge.common.metadata import MetadataStore
from dataforge.ingestion.database.extract import extract_incremental_by_timestamp
from dataforge.transformations import pandas_transforms as P

_log = get_logger("dataforge.pipelines.incremental")

_PIPELINE = "incremental_transactions"
_SOURCE_TABLE = "transactions"
_FACT_TABLE = "fact_transaction_incr"


def _warehouse_conn(config: Config) -> sqlite3.Connection:
    path = config.path("warehouse.local_path")
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_FACT_TABLE} (
            transaction_id TEXT PRIMARY KEY,
            account_id     TEXT,
            amount         REAL,
            currency       TEXT,
            status         TEXT,
            fraud_flag     INTEGER,
            updated_at     TEXT
        )
        """)
    return conn


def _merge(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """UPSERT rows into the fact table (INSERT ... ON CONFLICT DO UPDATE)."""
    cols = [
        "transaction_id",
        "account_id",
        "amount",
        "currency",
        "status",
        "fraud_flag",
        "updated_at",
    ]
    subset = df[cols].copy()
    subset["updated_at"] = subset["updated_at"].astype(str)
    rows = list(subset.itertuples(index=False, name=None))
    conn.executemany(
        f"""
        INSERT INTO {_FACT_TABLE}
            (transaction_id, account_id, amount, currency, status, fraud_flag, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(transaction_id) DO UPDATE SET
            amount=excluded.amount, status=excluded.status,
            fraud_flag=excluded.fraud_flag, updated_at=excluded.updated_at
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def run(config: Config | None = None) -> dict[str, int]:
    """Run one incremental cycle; returns rows extracted/merged."""
    config = config or load_config()
    metadata = MetadataStore.from_config(config)
    metadata.register_pipeline(
        _PIPELINE,
        source=f"db.{_SOURCE_TABLE}",
        target=f"warehouse.{_FACT_TABLE}",
        load_type="incremental_ts",
    )

    result = {"extracted": 0, "merged": 0}
    with metadata.run(_PIPELINE) as handle:
        # 1-2) extract changed rows since watermark
        df, new_wm = extract_incremental_by_timestamp(
            _SOURCE_TABLE, pipeline_name=_PIPELINE, config=config, metadata=metadata
        )
        handle.records_read = len(df)
        result["extracted"] = len(df)

        if not df.empty:
            # 3) transform: drop rows that would violate the PK / are invalid
            clean = df[df["account_id"].notna()].copy()
            clean = P.cast_types(clean, {"amount": "decimal", "fraud_flag": "int"})
            clean = P.deduplicate(clean, ["transaction_id"], order_by="updated_at")

            # 4) MERGE into the warehouse
            conn = _warehouse_conn(config)
            try:
                merged = _merge(conn, clean)
            finally:
                conn.close()
            handle.records_written = merged
            result["merged"] = merged

        # 5) commit watermark ONLY after successful load
        metadata.commit_watermark(_PIPELINE, _SOURCE_TABLE)

    _log.info("incremental cycle complete", extra=result)
    return result


def main() -> int:
    r1 = run()
    print(f"\nFirst run : extracted={r1['extracted']:,} merged={r1['merged']:,}")
    r2 = run()
    print(
        f"Second run: extracted={r2['extracted']:,} merged={r2['merged']:,} "
        f"(0 expected - watermark advanced)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
