"""Database extraction patterns (full + incremental)."""

from dataforge.ingestion.database.extract import (
    extract_full,
    extract_incremental_by_id,
    extract_incremental_by_timestamp,
)

__all__ = [
    "extract_full",
    "extract_incremental_by_timestamp",
    "extract_incremental_by_id",
]
