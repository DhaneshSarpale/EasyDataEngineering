"""End-to-end local batch pipeline: RAW -> BRONZE -> SILVER -> GOLD.

Demonstrates the medallion data-lake architecture on the local filesystem
lake (mirroring the S3 zone layout). Each layer has a clear contract:

- **RAW**    : source bytes/records exactly as extracted. Immutable audit.
- **BRONZE** : parsed into typed columns, minimal cleaning, provenance cols.
- **SILVER** : cleansed, deduplicated, conformed, quality-checked. Bad rows
               are split out to the quarantine zone with a reason.
- **GOLD**   : business marts - the star schema (dims + facts) and the
               daily aggregate summary.

Run:  ``make run-local``  or  ``python -m dataforge.pipelines.local_batch_pipeline``

The whole run is tracked in the metadata store (records read/written/failed)
so it produces the same observability signal a real pipeline would.
"""

from __future__ import annotations

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.lake import LocalLake
from dataforge.common.logging_utils import get_logger
from dataforge.common.metadata import MetadataStore
from dataforge.data_quality.engine import run_expectations
from dataforge.data_quality.rules import transaction_rules
from dataforge.ingestion.database.extract import extract_full
from dataforge.transformations import pandas_transforms as P
from dataforge.transformations import star_schema as S

_log = get_logger("dataforge.pipelines.local_batch")

_PIPELINE = "local_batch_pipeline"


def run(config: Config | None = None) -> dict[str, int]:
    """Execute the full batch pipeline and return a summary of row counts."""
    config = config or load_config()
    lake = LocalLake(config)
    metadata = MetadataStore.from_config(config)
    metadata.register_pipeline(_PIPELINE, source="source.sqlite", target="gold.*", load_type="full")
    summary: dict[str, int] = {}

    with metadata.run(_PIPELINE) as handle:
        # ---------------- RAW -> BRONZE ----------------
        tables = {}
        for name in (
            "transactions",
            "accounts",
            "customers",
            "merchants",
            "branches",
        ):
            df = extract_full(name, config=config)
            lake.write_parquet(df, "bronze", name)
            tables[name] = df
            summary[f"bronze_{name}"] = len(df)
        handle.records_read = len(tables["transactions"])

        # ---------------- BRONZE -> SILVER ----------------
        txn = tables["transactions"]

        # Data-quality split: good rows continue, bad rows -> quarantine.
        results = run_expectations(txn, transaction_rules(config), config=config)
        bad_mask = results.row_failure_mask(txn)
        good = txn.loc[~bad_mask].copy()
        bad = txn.loc[bad_mask].copy()
        if not bad.empty:
            bad = bad.assign(_dq_reason=results.reason_for_rows(txn).loc[bad.index])
            lake.quarantine(bad, "transactions")
        handle.records_failed = int(bad_mask.sum())

        # Cleanse + conform + dedup.
        silver_txn = P.cast_types(
            good,
            {"amount": "decimal", "transaction_timestamp": "timestamp", "fraud_flag": "int"},
        )
        silver_txn = P.deduplicate(silver_txn, ["transaction_id"], order_by="updated_at")
        lake.write_parquet(
            _with_partitions(silver_txn),
            "silver",
            "transactions",
            partition_cols=["year", "month"],
        )
        summary["silver_transactions"] = len(silver_txn)

        for name in ("customers", "accounts", "merchants", "branches"):
            lake.write_parquet(tables[name], "silver", name)

        # ---------------- SILVER -> GOLD (star schema + marts) ----------------
        dim_customer = S.build_dim_customer(tables["customers"])
        dim_account = S.build_dim_account(tables["accounts"])
        dim_merchant = S.build_dim_merchant(tables["merchants"])
        dim_branch = S.build_dim_branch(tables["branches"])
        dim_date = S.build_dim_date(silver_txn)
        fact_txn = S.build_fact_transaction(silver_txn, dim_account, dim_merchant)

        for name, dim in {
            "dim_customer": dim_customer,
            "dim_account": dim_account,
            "dim_merchant": dim_merchant,
            "dim_branch": dim_branch,
            "dim_date": dim_date,
            "fact_transaction": fact_txn,
        }.items():
            lake.write_parquet(dim, "gold", name)
            summary[name] = len(dim)

        daily = P.aggregate_daily_transactions(silver_txn)
        lake.write_parquet(daily, "gold", "daily_transaction_summary")
        summary["daily_transaction_summary"] = len(daily)

        handle.records_written = len(fact_txn)

    _log.info("local batch pipeline complete", extra={"summary": summary})
    return summary


def _with_partitions(df: pd.DataFrame) -> pd.DataFrame:
    """Add year/month partition columns derived from the transaction date."""
    out = df.copy()
    ts = pd.to_datetime(out["transaction_timestamp"], errors="coerce", utc=True)
    out["year"] = ts.dt.year.astype("Int64")
    out["month"] = ts.dt.month.astype("Int64")
    return out.dropna(subset=["year", "month"])


def main() -> int:
    summary = run()
    print("\nLocal batch pipeline summary:")
    for k, v in summary.items():
        print(f"  {k:28s} {v:>10,d}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
