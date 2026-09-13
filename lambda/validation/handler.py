"""Lambda: pipeline validation + metadata steps for Step Functions.

Invoked by the state machine at several stages (``validate_source``,
``validate_schema``, ``update_metadata``). It reuses the DataForge package
so the same logic runs locally and in Lambda. The ``stage`` field in the
payload selects the behaviour.
"""

from __future__ import annotations

from typing import Any


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    stage = event.get("stage", "validate_source")
    table = event.get("table", "transactions")

    if stage == "validate_source":
        return _validate_source(table)
    if stage == "validate_schema":
        return _validate_schema(table)
    if stage == "update_metadata":
        return _update_metadata(table)
    return {"ok": False, "reason": f"unknown stage {stage}"}


def _validate_source(table: str) -> dict[str, Any]:
    """Confirm the source has rows to process."""
    from dataforge.ingestion.database.extract import extract_full

    try:
        df = extract_full(table)
        return {"ok": True, "table": table, "row_count": len(df)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "table": table, "error": str(exc)}


def _validate_schema(table: str) -> dict[str, Any]:
    """Assert the required columns are present."""
    from dataforge.ingestion.database.extract import extract_full

    required = {
        "transactions": ["transaction_id", "account_id", "amount", "currency"],
    }.get(table, [])
    df = extract_full(table)
    missing = [c for c in required if c not in df.columns]
    return {"ok": not missing, "table": table, "missing_columns": missing}


def _update_metadata(table: str) -> dict[str, Any]:
    """Commit the watermark after a successful load."""
    from dataforge.common.metadata import MetadataStore

    store = MetadataStore.from_config()
    store.commit_watermark(f"incr_ts_{table}", table)
    return {"ok": True, "table": table, "watermark_committed": True}
