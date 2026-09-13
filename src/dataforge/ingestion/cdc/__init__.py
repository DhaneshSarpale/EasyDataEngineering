"""Change Data Capture (CDC) pattern + DMS reference."""

from dataforge.ingestion.cdc.cdc import generate_cdc_events, reconstruct_latest_state

__all__ = ["generate_cdc_events", "reconstruct_latest_state"]
