"""Batch ingestion: full-table extract -> lake bronze zone.

Ties the database extractor to the lake writer and wraps the whole thing
in run tracking so ``pipeline_run`` records how many rows moved. This is
the "daily full load of a dimension" pattern.
"""

from __future__ import annotations

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.lake import LocalLake
from dataforge.common.logging_utils import get_logger
from dataforge.common.metadata import MetadataStore
from dataforge.ingestion.database.extract import extract_full

_log = get_logger("dataforge.ingestion.batch")


def ingest_table_full(
    table: str,
    *,
    partition_cols: list[str] | None = None,
    config: Config | None = None,
) -> pd.DataFrame:
    """Full-extract ``table`` and land it in the bronze zone as Parquet."""
    config = config or load_config()
    lake = LocalLake(config)
    metadata = MetadataStore.from_config(config)
    pipeline_name = f"batch_full_{table}"
    metadata.register_pipeline(
        pipeline_name, source=f"db.{table}", target=f"bronze.{table}", load_type="full"
    )

    with metadata.run(pipeline_name) as handle:
        df = extract_full(table, config=config)
        handle.records_read = len(df)
        lake.write_parquet(df, "bronze", table, partition_cols=partition_cols)
        handle.records_written = len(df)

    _log.info("batch full ingest done", extra={"table": table, "rows": len(df)})
    return df
