"""Database extraction patterns 1-3.

Pattern 1 - Full extraction
    ``SELECT * FROM <table>`` -> land the whole table. Simple, but re-reads
    everything every run. Fine for small dimensions; expensive for large
    fact tables.

Pattern 2 - Incremental by timestamp
    ``WHERE updated_at > :last_success_timestamp``. Uses the metadata
    watermark. Only rows changed since the last successful run are pulled.
    The watermark is committed *after* a successful load, so a failed run
    reprocesses the same window (idempotent reruns).

Pattern 3 - Incremental by id (watermarking)
    ``WHERE customer_id > :last_processed_id``. Works when a monotonically
    increasing key exists. Cheaper index scan; but cannot detect UPDATEs to
    already-seen rows (use timestamp or CDC for that).

LOCAL MODE reads the generated ``source.sqlite``. AWS MODE would issue the
same SQL over JDBC (Glue) or psycopg2/SQLAlchemy against RDS - the pattern
is identical, only the connection differs. See ``jdbc_reference.py``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger
from dataforge.common.metadata import MetadataStore

_log = get_logger("dataforge.ingestion.database")


def _connect(config: Config) -> sqlite3.Connection:
    db_path = config.path("source_db.local_path")
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Source DB not found at {db_path}. Run: make generate-data")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def extract_full(table: str, config: Config | None = None) -> pd.DataFrame:
    """Pattern 1: extract an entire table."""
    config = config or load_config()
    _validate_identifier(table)
    with _connect(config) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)  # noqa: S608 - identifier validated
    _log.info("full extract", extra={"table": table, "rows": len(df)})
    return df


def extract_incremental_by_timestamp(
    table: str,
    *,
    timestamp_col: str = "updated_at",
    pipeline_name: str | None = None,
    config: Config | None = None,
    metadata: MetadataStore | None = None,
) -> tuple[pd.DataFrame, str]:
    """Pattern 2: extract only rows changed since the last watermark.

    Returns ``(dataframe, new_watermark)``. The caller commits the watermark
    via :meth:`MetadataStore.commit_watermark` only *after* the records are
    successfully persisted.
    """
    config = config or load_config()
    metadata = metadata or MetadataStore.from_config(config)
    pipeline_name = pipeline_name or f"incr_ts_{table}"
    _validate_identifier(table)
    _validate_identifier(timestamp_col)

    last_success = metadata.get_watermark(pipeline_name, table)
    with _connect(config) as conn:
        if last_success is None:
            query = f"SELECT * FROM {table} ORDER BY {timestamp_col}"  # noqa: S608
            df = pd.read_sql_query(query, conn)
        else:
            query = (
                f"SELECT * FROM {table} WHERE {timestamp_col} > ? "  # noqa: S608
                f"ORDER BY {timestamp_col}"
            )
            df = pd.read_sql_query(query, conn, params=(last_success,))

    new_watermark = str(df[timestamp_col].max()) if not df.empty else (last_success or "")
    metadata.begin_watermark(pipeline_name, table, new_watermark)
    _log.info(
        "incremental extract by timestamp",
        extra={
            "table": table,
            "rows": len(df),
            "since": last_success,
            "new_watermark": new_watermark,
        },
    )
    return df, new_watermark


def extract_incremental_by_id(
    table: str,
    *,
    id_col: str,
    pipeline_name: str | None = None,
    config: Config | None = None,
    metadata: MetadataStore | None = None,
) -> tuple[pd.DataFrame, str]:
    """Pattern 3: extract only rows with id greater than the last processed id.

    Ids in this project are zero-padded strings (e.g. ``CUST00000042``)
    which sort lexicographically in the same order as numerically because
    of the fixed width - so ``>`` comparison is valid.
    """
    config = config or load_config()
    metadata = metadata or MetadataStore.from_config(config)
    pipeline_name = pipeline_name or f"incr_id_{table}"
    _validate_identifier(table)
    _validate_identifier(id_col)

    last_id = metadata.get_watermark(pipeline_name, table)
    with _connect(config) as conn:
        if last_id is None:
            query = f"SELECT * FROM {table} ORDER BY {id_col}"  # noqa: S608
            df = pd.read_sql_query(query, conn)
        else:
            query = f"SELECT * FROM {table} WHERE {id_col} > ? ORDER BY {id_col}"  # noqa: S608
            df = pd.read_sql_query(query, conn, params=(last_id,))

    new_id = str(df[id_col].max()) if not df.empty else (last_id or "")
    metadata.begin_watermark(pipeline_name, table, new_id)
    _log.info(
        "incremental extract by id",
        extra={"table": table, "rows": len(df), "since_id": last_id, "new_id": new_id},
    )
    return df, new_id


def _validate_identifier(identifier: str) -> None:
    """Guard against SQL injection when a table/column name is interpolated.

    We only ever interpolate *identifiers* (never values) and only from a
    conservative character set. Values always go through bound parameters.
    """
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {identifier!r}")
