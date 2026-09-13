"""Runnable end-to-end pipelines (LOCAL MODE).

- ``local_batch_pipeline``  RAW -> BRONZE -> SILVER -> GOLD + star schema
- ``incremental_pipeline``  watermark extract -> transform -> MERGE + commit
"""
