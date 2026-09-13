"""Pipeline metadata & watermark control store.

Every serious data platform keeps a *control plane* separate from the
data plane. DataForge stores:

- ``pipeline_metadata``  : static registration of each pipeline
- ``pipeline_run``       : one row per execution (records read/written/failed)
- ``watermark``          : last-successful high-water mark per pipeline/table

Locally this is a SQLite database (``config.metadata.local_path``). In
AWS this maps naturally to a DynamoDB table or an RDS control schema; the
same interface applies.

Watermarks power incremental extraction. The watermark is only advanced
*after* a run succeeds, which is what makes reruns safe (idempotent) - a
failed run leaves the watermark untouched so the next run reprocesses the
same window.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.metadata")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    pipeline_id   TEXT PRIMARY KEY,
    pipeline_name TEXT UNIQUE NOT NULL,
    source        TEXT,
    target        TEXT,
    load_type     TEXT,
    schedule      TEXT,
    owner         TEXT,
    status        TEXT DEFAULT 'ACTIVE'
);

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id          TEXT PRIMARY KEY,
    pipeline_name   TEXT NOT NULL,
    start_time      TEXT NOT NULL,
    end_time        TEXT,
    records_read    INTEGER DEFAULT 0,
    records_written INTEGER DEFAULT 0,
    records_failed  INTEGER DEFAULT 0,
    status          TEXT NOT NULL,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS watermark (
    pipeline_name   TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    last_success_value TEXT,
    current_run_value  TEXT,
    updated_at      TEXT,
    status          TEXT,
    PRIMARY KEY (pipeline_name, source_table)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunHandle:
    """Represents an in-flight pipeline run; use via :meth:`MetadataStore.run`."""

    run_id: str
    pipeline_name: str
    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0


class MetadataStore:
    """SQLite-backed control store for runs and watermarks."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @classmethod
    def from_config(cls, config: Config | None = None) -> MetadataStore:
        config = config or load_config()
        return cls(config.path("metadata.local_path"))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------
    # Pipeline registration
    # ------------------------------------------------------------------
    def register_pipeline(
        self,
        pipeline_name: str,
        *,
        source: str = "",
        target: str = "",
        load_type: str = "full",
        schedule: str = "",
        owner: str = "data-eng",
    ) -> str:
        """Idempotently register a pipeline, returning its pipeline_id."""
        pipeline_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, pipeline_name))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_metadata
                    (pipeline_id, pipeline_name, source, target, load_type, schedule, owner)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pipeline_name) DO UPDATE SET
                    source=excluded.source, target=excluded.target,
                    load_type=excluded.load_type, schedule=excluded.schedule,
                    owner=excluded.owner
                """,
                (pipeline_id, pipeline_name, source, target, load_type, schedule, owner),
            )
        return pipeline_id

    # ------------------------------------------------------------------
    # Run tracking (context manager)
    # ------------------------------------------------------------------
    @contextmanager
    def run(self, pipeline_name: str) -> Iterator[RunHandle]:
        """Track a run. Marks SUCCESS on clean exit, FAILED on exception."""
        run_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pipeline_run (run_id, pipeline_name, start_time, status) "
                "VALUES (?, ?, ?, 'RUNNING')",
                (run_id, pipeline_name, _now()),
            )
        handle = RunHandle(run_id=run_id, pipeline_name=pipeline_name)
        _log.info("run started", extra={"run_id": run_id, "pipeline": pipeline_name})
        try:
            yield handle
        except Exception as exc:  # noqa: BLE001 - we re-raise after recording
            self._finish_run(handle, status="FAILED", error=str(exc))
            _log.error(
                "run failed",
                extra={"run_id": run_id, "pipeline": pipeline_name, "error": str(exc)},
            )
            raise
        else:
            self._finish_run(handle, status="SUCCESS")
            _log.info(
                "run succeeded",
                extra={
                    "run_id": run_id,
                    "pipeline": pipeline_name,
                    "records_read": handle.records_read,
                    "records_written": handle.records_written,
                    "records_failed": handle.records_failed,
                },
            )

    def _finish_run(self, handle: RunHandle, *, status: str, error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE pipeline_run
                SET end_time=?, records_read=?, records_written=?, records_failed=?,
                    status=?, error_message=?
                WHERE run_id=?
                """,
                (
                    _now(),
                    handle.records_read,
                    handle.records_written,
                    handle.records_failed,
                    status,
                    error,
                    handle.run_id,
                ),
            )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM pipeline_run WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Watermarks
    # ------------------------------------------------------------------
    def get_watermark(self, pipeline_name: str, source_table: str) -> str | None:
        """Return the last successful watermark value, or None if never run."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_success_value FROM watermark "
                "WHERE pipeline_name=? AND source_table=?",
                (pipeline_name, source_table),
            ).fetchone()
        return row["last_success_value"] if row else None

    def begin_watermark(self, pipeline_name: str, source_table: str, current_value: str) -> None:
        """Record the value this run intends to advance the watermark *to*."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watermark
                    (pipeline_name, source_table, current_run_value, updated_at, status)
                VALUES (?, ?, ?, ?, 'RUNNING')
                ON CONFLICT(pipeline_name, source_table) DO UPDATE SET
                    current_run_value=excluded.current_run_value,
                    updated_at=excluded.updated_at, status='RUNNING'
                """,
                (pipeline_name, source_table, current_value, _now()),
            )

    def commit_watermark(self, pipeline_name: str, source_table: str) -> None:
        """Promote current_run_value to last_success_value (only after success)."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE watermark
                SET last_success_value = current_run_value,
                    status = 'SUCCESS', updated_at = ?
                WHERE pipeline_name=? AND source_table=?
                """,
                (_now(), pipeline_name, source_table),
            )
        _log.info(
            "watermark committed",
            extra={"pipeline": pipeline_name, "source_table": source_table},
        )
