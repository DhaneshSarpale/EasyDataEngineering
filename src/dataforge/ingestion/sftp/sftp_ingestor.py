"""SFTP file ingestion.

Banks routinely exchange files over SFTP. The ingestion lifecycle is:

1. **discovery**   list remote files matching a pattern
2. **download**    pull each file to a local landing area
3. **validation**  non-empty + parseable + checksum recorded
4. **idempotency** skip files already processed (checksum ledger)
5. **archive**     move processed files aside so they aren't re-pulled
6. **failure**     bad files -> quarantine + reason, pipeline continues

LOCAL MODE simulates the remote server with a local directory
(``config.sftp.local_landing``) so the exact same logic runs without a
real SFTP server. AWS MODE uses **AWS Transfer Family** (managed SFTP that
lands directly in S3) or a ``paramiko`` client - see :data:`PARAMIKO_REFERENCE`.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from dataforge.common.config import Config, load_config
from dataforge.common.idempotency import IdempotencyStore, file_checksum
from dataforge.common.lake import LocalLake
from dataforge.common.logging_utils import get_logger
from dataforge.ingestion.files.file_ingestor import FileIngestor

_log = get_logger("dataforge.ingestion.sftp")


@dataclass
class SftpResult:
    file: str
    status: str  # PROCESSED | SKIPPED_DUPLICATE | QUARANTINED
    checksum: str = ""
    reason: str = ""


class SftpIngestor:
    """Simulated SFTP ingestion driven by the local landing directory."""

    def __init__(
        self,
        dataset: str,
        *,
        required_columns: list[str] | None = None,
        config: Config | None = None,
    ) -> None:
        self.dataset = dataset
        self.config = config or load_config()
        self.remote_dir = self.config.path("sftp.local_landing")
        self.archive_dir = self.config.path("sftp.archive_dir")
        self.idem = IdempotencyStore.from_config(self.config)
        self.lake = LocalLake(self.config)
        self.file_ingestor = FileIngestor(
            dataset, required_columns=required_columns, config=self.config
        )

    def discover(self, pattern: str = "*.csv") -> list[Path]:
        """List remote files awaiting ingestion."""
        if not self.remote_dir.exists():
            return []
        files = sorted(p for p in self.remote_dir.glob(pattern) if p.is_file())
        _log.info("sftp discovery", extra={"count": len(files), "dir": str(self.remote_dir)})
        return files

    def ingest(self, pattern: str = "*.csv") -> list[SftpResult]:
        """Discover, validate (checksum + idempotency), ingest and archive."""
        results: list[SftpResult] = []
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        for remote in self.discover(pattern):
            checksum = file_checksum(remote)

            if self.idem.is_file_processed(remote.name, checksum):
                results.append(SftpResult(remote.name, "SKIPPED_DUPLICATE", checksum=checksum))
                continue

            outcome = self.file_ingestor.ingest_file(remote)
            status = outcome.status
            reason = outcome.reason

            if status == "PROCESSED":
                shutil.copy2(remote, self.archive_dir / remote.name)

            results.append(SftpResult(remote.name, status, checksum=checksum, reason=reason))

        _log.info(
            "sftp ingestion complete",
            extra={
                "processed": sum(r.status == "PROCESSED" for r in results),
                "skipped": sum(r.status == "SKIPPED_DUPLICATE" for r in results),
                "quarantined": sum(r.status == "QUARANTINED" for r in results),
            },
        )
        return results


PARAMIKO_REFERENCE = r"""
import paramiko
from dataforge.common.secrets import get_secret

def download_all(host, port, username, remote_dir, local_dir):
    password = get_secret("SFTP_PASSWORD", aws_secret_name="dataforge/dev/sftp")
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        for name in sftp.listdir(remote_dir):
            sftp.get(f"{remote_dir}/{name}", f"{local_dir}/{name}")
    finally:
        sftp.close()
        transport.close()

# Preferred on AWS: AWS Transfer Family gives a managed SFTP endpoint that
# writes uploaded files straight into S3 (no server to run). An S3
# PutObject event then triggers a Lambda that starts the Glue job.
"""
