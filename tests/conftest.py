"""Shared pytest fixtures.

Tests run in complete isolation: a session-scoped fixture generates a tiny
synthetic dataset into a temp directory and points the DataForge config's
paths (source DB, lake, warehouse, metadata) at it via monkeypatched
config. This means the suite never touches the developer's real
``data/generated`` and is safe to run in CI from a clean checkout.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from dataforge.common.config import load_config
from dataforge.data_generation.generate import generate_all
from dataforge.data_generation.generators import GeneratorConfig


@pytest.fixture(scope="session")
def tiny_cfg() -> GeneratorConfig:
    """A very small, deterministic generation profile for fast tests."""
    return GeneratorConfig(
        customers=200,
        accounts=400,
        transactions=2000,
        cards=200,
        loans=100,
        merchants=60,
        branches=10,
        employees=40,
        payments=500,
        fraud_events=40,
        bad_data_ratio=0.03,
        seed=123,
    )


@pytest.fixture(scope="session")
def tables(tiny_cfg: GeneratorConfig) -> dict[str, pd.DataFrame]:
    """All generated tables as in-memory DataFrames (no disk needed)."""
    return generate_all(tiny_cfg)


@pytest.fixture()
def isolated_env(tmp_path: Path, tables: dict[str, pd.DataFrame], monkeypatch):
    """Point the DataForge config at a temp source DB + lake + warehouse.

    Yields the loaded Config with its paths rewritten to ``tmp_path`` so
    ingestion/pipeline tests read/write only inside the temp dir.
    """
    # Build a source SQLite DB from the generated tables.
    source_db = tmp_path / "source.sqlite"
    conn = sqlite3.connect(source_db)
    try:
        core = [
            "customers",
            "accounts",
            "transactions",
            "cards",
            "loans",
            "merchants",
            "branches",
            "employees",
            "payments",
            "fraud_events",
        ]
        for name in core:
            df = tables[name].copy()
            for col in df.select_dtypes(include=["datetimetz", "datetime"]).columns:
                df[col] = df[col].astype(str)
            df.to_sql(name, conn, if_exists="replace", index=False)
        conn.commit()
    finally:
        conn.close()

    cfg = load_config("dev")
    # Rewrite the mutable raw config dict to temp paths.
    raw = dict(cfg.raw)
    raw["source_db"] = {**raw["source_db"], "local_path": str(source_db)}
    raw["metadata"] = {**raw["metadata"], "local_path": str(tmp_path / "metadata.sqlite")}
    raw["warehouse"] = {**raw["warehouse"], "local_path": str(tmp_path / "wh.sqlite")}
    lake_root = tmp_path / "lake"
    raw["local_lake"] = {
        "root": str(lake_root),
        "raw": str(lake_root / "raw"),
        "bronze": str(lake_root / "bronze"),
        "silver": str(lake_root / "silver"),
        "gold": str(lake_root / "gold"),
        "quarantine": str(lake_root / "quarantine"),
    }
    object.__setattr__(cfg, "raw", raw)
    # repo_root stays for config.path() of already-absolute temp paths.
    return cfg
