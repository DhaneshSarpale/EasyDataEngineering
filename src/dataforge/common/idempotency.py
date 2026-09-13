"""Idempotency tracking - process each file/event exactly once.

Reprocessing the same input twice corrupts aggregates (double counting)
and wastes compute. DataForge guards against this with a small ledger:

- **Files**: identified by a content checksum (sha256) *and* logical
  file_id. If the checksum was seen before, the file is skipped.
- **Events**: identified by ``event_id``. A consumer records processed
  ids and drops duplicates (at-least-once delivery -> effectively
  once-processed).

Locally this ledger is SQLite. In AWS a natural fit is DynamoDB with a
conditional ``PutItem`` (attribute_not_exists) which gives atomic
"claim-once" semantics without race conditions.
"""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.idempotency")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_files (
    file_id      TEXT NOT NULL,
    checksum     TEXT NOT NULL,
    status       TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (file_id, checksum)
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_checksum(path: str | Path, chunk_size: int = 1 << 20) -> str:
    """Compute a streaming sha256 checksum of a file's contents."""
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


class IdempotencyStore:
    """Ledger of processed files and events."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    @classmethod
    def from_config(cls, config: Config | None = None) -> IdempotencyStore:
        config = config or load_config()
        return cls(config.path("metadata.local_path"))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ------------------------------------------------------------------
    # Files
    # ------------------------------------------------------------------
    def is_file_processed(self, file_id: str, checksum: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_files "
                "WHERE file_id=? AND checksum=? AND status='PROCESSED'",
                (file_id, checksum),
            ).fetchone()
        return row is not None

    def mark_file(self, file_id: str, checksum: str, status: str = "PROCESSED") -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO processed_files (file_id, checksum, status, processed_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(file_id, checksum) DO UPDATE SET "
                "status=excluded.status, processed_at=excluded.processed_at",
                (file_id, checksum, status, _now()),
            )

    def claim_file(self, path: str | Path, file_id: str | None = None) -> bool:
        """Return True if this file is new (caller should process it)."""
        path = Path(path)
        file_id = file_id or path.name
        checksum = file_checksum(path)
        if self.is_file_processed(file_id, checksum):
            _log.info(
                "duplicate file skipped",
                extra={"file_id": file_id, "checksum": checksum[:12]},
            )
            return False
        return True

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def is_event_processed(self, event_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM processed_events WHERE event_id=?", (event_id,)
            ).fetchone()
        return row is not None

    def mark_event(self, event_id: str) -> bool:
        """Atomically claim an event id. Returns True if newly claimed.

        A False return means the event was a duplicate and should be dropped.
        """
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)",
                    (event_id, _now()),
                )
            return True
        except sqlite3.IntegrityError:
            return False
