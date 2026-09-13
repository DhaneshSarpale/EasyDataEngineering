"""Local data-lake I/O helpers mirroring the S3 zone layout.

The lake has four zones plus quarantine:

- **raw**    : bytes exactly as received (CSV/JSON/XML). Immutable, audit.
- **bronze** : raw parsed into typed columns, minimal cleaning.
- **silver** : cleansed, deduplicated, conformed, quality-checked.
- **gold**   : business-level aggregates / star-schema marts.
- **quarantine** : rejected records with a reason, for investigation.

Locally each zone is a directory under ``data/lake/``. Parquet with
Snappy compression is the default columnar format (fast, splittable,
schema-carrying). Partitioning by ``year/month/day`` enables partition
pruning - the exact same physical layout used in S3.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from dataforge.common.config import Config, load_config
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.lake")

_ZONES = ("raw", "bronze", "silver", "gold", "quarantine")


class LocalLake:
    """Thin wrapper over the local lake directory structure."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or load_config()
        self.root = self.config.path("local_lake.root")
        for zone in _ZONES:
            (self.root / zone).mkdir(parents=True, exist_ok=True)

    def zone_path(self, zone: str, dataset: str) -> Path:
        if zone not in _ZONES:
            raise ValueError(f"Unknown zone '{zone}'. Expected one of {_ZONES}.")
        return self.root / zone / dataset

    def write_parquet(
        self,
        df: pd.DataFrame,
        zone: str,
        dataset: str,
        *,
        partition_cols: Iterable[str] | None = None,
        compression: str = "snappy",
    ) -> Path:
        """Write a DataFrame to a lake zone as Parquet.

        With ``partition_cols`` the dataset is written as a Hive-partitioned
        directory (``dataset/col=val/...``). Without partitions it is written
        as a directory containing a single ``part-000.parquet`` file, so the
        physical layout (a directory per dataset) is consistent either way
        and mirrors how S3 "prefixes" behave.
        """
        target = self.zone_path(zone, dataset)
        target.mkdir(parents=True, exist_ok=True)
        partition_cols = list(partition_cols or [])
        if partition_cols:
            df.to_parquet(
                target,
                engine="pyarrow",
                compression=compression,
                index=False,
                partition_cols=partition_cols,
            )
        else:
            df.to_parquet(
                target / "part-000.parquet",
                engine="pyarrow",
                compression=compression,
                index=False,
            )
        _log.info(
            "wrote parquet",
            extra={
                "zone": zone,
                "dataset": dataset,
                "rows": len(df),
                "partitions": partition_cols,
            },
        )
        return target

    def read_parquet(self, zone: str, dataset: str) -> pd.DataFrame:
        """Read a (possibly partitioned) Parquet dataset back into pandas."""
        target = self.zone_path(zone, dataset)
        return pd.read_parquet(target, engine="pyarrow")

    def quarantine(self, df: pd.DataFrame, dataset: str, reason_col: str = "_dq_reason") -> Path:
        """Persist rejected records (with a reason) to the quarantine zone."""
        if reason_col not in df.columns:
            df = df.assign(**{reason_col: "unspecified"})
        target = self.zone_path("quarantine", dataset)
        target.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            target / "part-000.parquet", engine="pyarrow", compression="snappy", index=False
        )
        _log.warning("records quarantined", extra={"dataset": dataset, "rows": len(df)})
        return target
