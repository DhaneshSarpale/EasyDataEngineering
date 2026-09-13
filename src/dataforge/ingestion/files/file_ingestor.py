"""Robust file ingestion into the lake bronze zone.

Handles the messy realities of file-based ingestion:

- **schema detection**   infer columns/dtypes from the file
- **schema validation**  assert required columns are present
- **corrupt files**      malformed content -> quarantine, keep going
- **duplicate files**    same checksum already processed -> skip (idempotent)
- **empty files**        zero bytes -> quarantine with reason
- **malformed records**  bad rows routed out, good rows kept

Returns an :class:`IngestResult` per file so a directory sweep can report
processed / skipped / quarantined counts.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.errors import DataForgeError
from dataforge.common.idempotency import IdempotencyStore, file_checksum
from dataforge.common.lake import LocalLake
from dataforge.common.logging_utils import get_logger
from dataforge.ingestion.files.readers import read_file, sniff_format

_log = get_logger("dataforge.ingestion.files")


@dataclass
class IngestResult:
    """Outcome of ingesting a single file."""

    path: str
    status: str  # PROCESSED | SKIPPED_DUPLICATE | QUARANTINED
    rows: int = 0
    reason: str = ""


@dataclass
class SweepReport:
    """Aggregate outcome of ingesting a directory of files."""

    results: list[IngestResult] = field(default_factory=list)

    @property
    def processed(self) -> int:
        return sum(1 for r in self.results if r.status == "PROCESSED")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.status == "SKIPPED_DUPLICATE")

    @property
    def quarantined(self) -> int:
        return sum(1 for r in self.results if r.status == "QUARANTINED")


class FileIngestor:
    """Ingest files into the lake with validation + idempotency."""

    def __init__(
        self,
        dataset: str,
        *,
        required_columns: list[str] | None = None,
        config: Config | None = None,
    ) -> None:
        self.dataset = dataset
        self.required_columns = required_columns or []
        self.config = config or load_config()
        self.lake = LocalLake(self.config)
        self.idem = IdempotencyStore.from_config(self.config)

    def ingest_file(self, path: str | Path) -> IngestResult:
        """Ingest one file, returning its outcome (never raises for bad data)."""
        path = Path(path)
        try:
            checksum = file_checksum(path)
        except OSError as exc:
            return IngestResult(str(path), "QUARANTINED", reason=f"unreadable: {exc}")

        if self.idem.is_file_processed(path.name, checksum):
            return IngestResult(str(path), "SKIPPED_DUPLICATE")

        try:
            fmt = sniff_format(path)
            df = read_file(path, fmt)
        except DataForgeError as exc:
            self.idem.mark_file(path.name, checksum, status="QUARANTINED")
            self._quarantine_reason(path, str(exc))
            return IngestResult(str(path), "QUARANTINED", reason=str(exc))

        missing = [c for c in self.required_columns if c not in df.columns]
        if missing:
            reason = f"missing required columns: {missing}"
            self.idem.mark_file(path.name, checksum, status="QUARANTINED")
            self._quarantine_reason(path, reason)
            return IngestResult(str(path), "QUARANTINED", reason=reason)

        df = df.assign(_source_file=path.name, _ingested_checksum=checksum)
        self.lake.write_parquet(df, "bronze", self.dataset)
        self.idem.mark_file(path.name, checksum, status="PROCESSED")
        _log.info("file ingested", extra={"file": path.name, "rows": len(df)})
        return IngestResult(str(path), "PROCESSED", rows=len(df))

    def ingest_dir(self, directory: str | Path, pattern: str = "*") -> SweepReport:
        """Ingest every matching file in a directory."""
        directory = Path(directory)
        report = SweepReport()
        for file in sorted(directory.glob(pattern)):
            if file.is_file():
                report.results.append(self.ingest_file(file))
        _log.info(
            "directory sweep complete",
            extra={
                "dir": str(directory),
                "processed": report.processed,
                "skipped": report.skipped,
                "quarantined": report.quarantined,
            },
        )
        return report

    def _quarantine_reason(self, path: Path, reason: str) -> None:
        """Record a rejected file's reason into the quarantine zone."""
        df = pd.DataFrame([{"file": path.name, "_dq_reason": reason}])
        self.lake.quarantine(df, f"{self.dataset}_rejected_files")
