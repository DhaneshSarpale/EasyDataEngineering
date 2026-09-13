"""CLI entry point that generates and materialises synthetic data.

Outputs (under ``data/generated/`` by default):

- ``source.sqlite``           relational source DB (for extraction demos)
- ``csv/*.csv``               branches, employees (file-ingestion CSV demo)
- ``json/events.json``        nested JSON (API/file-ingestion demo)
- ``xml/external.xml``        XML (file-ingestion XML demo)
- ``parquet/*.parquet``       columnar copies of every table
- ``customer_changes.csv``    updated customer rows for the SCD Type 2 demo
- ``sftp_inbox/*.csv``        daily branch files for the SFTP demo

Run::

    python -m dataforge.data_generation.generate --profile small
    make generate-data RECORDS=medium
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

from dataforge.common.config import load_config, load_data_profiles
from dataforge.common.logging_utils import get_logger
from dataforge.data_generation.generators import BankDataGenerator, GeneratorConfig

_log = get_logger("dataforge.data_generation")


def build_config(profile: str, seed: int) -> GeneratorConfig:
    """Translate a named volume profile into a GeneratorConfig."""
    profiles = load_data_profiles()
    if profile not in profiles["profiles"]:
        raise SystemExit(f"Unknown profile '{profile}'. Options: {list(profiles['profiles'])}")
    counts = profiles["profiles"][profile]
    return GeneratorConfig(
        bad_data_ratio=profiles.get("bad_data_ratio", 0.02),
        seed=seed,
        **counts,
    )


def generate_all(cfg: GeneratorConfig) -> dict[str, pd.DataFrame]:
    """Generate every table in dependency order and return them by name."""
    gen = BankDataGenerator(cfg)
    tables: dict[str, pd.DataFrame] = {}
    tables["branches"] = gen.gen_branches()
    tables["employees"] = gen.gen_employees()
    tables["merchants"] = gen.gen_merchants()
    tables["customers_clean"] = gen.gen_customers()
    tables["customers"] = gen.inject_bad_customers(tables["customers_clean"])
    tables["accounts"] = gen.gen_accounts()
    tables["cards"] = gen.gen_cards()
    tables["loans"] = gen.gen_loans()
    txns_clean = gen.gen_transactions()
    tables["transactions"] = gen.inject_bad_transactions(txns_clean)
    tables["payments"] = gen.gen_payments()
    tables["fraud_events"] = gen.gen_fraud_events(txns_clean)
    tables["customer_changes"] = gen.gen_customer_changes(tables["customers_clean"])
    return tables


def _write_sqlite(tables: dict[str, pd.DataFrame], db_path: Path) -> None:
    """Materialise the core relational tables into a SQLite source DB."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
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
    conn = sqlite3.connect(db_path)
    try:
        for name in core:
            df = tables[name].copy()
            for col in df.select_dtypes(include=["datetimetz", "datetime"]).columns:
                df[col] = df[col].astype(str)
            df.to_sql(name, conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_txn_updated ON transactions(updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_cust_updated ON customers(updated_at)")
        conn.commit()
    finally:
        conn.close()
    _log.info("source database written", extra={"path": str(db_path)})


def _write_csv(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tables["branches"].to_csv(out_dir / "branches.csv", index=False)
    tables["employees"].to_csv(out_dir / "employees.csv", index=False)
    tables["customer_changes"].to_csv(out_dir / "customer_changes.csv", index=False)


def _write_json(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    """Write a nested JSON document (device/event style) for flattening demos."""
    out_dir.mkdir(parents=True, exist_ok=True)
    sample = tables["transactions"].head(500).copy()
    for col in sample.select_dtypes(include=["datetimetz", "datetime"]).columns:
        sample[col] = sample[col].astype(str)
    events = []
    for _, row in sample.iterrows():
        events.append(
            {
                "event_id": row["transaction_id"],
                "device": {"channel": row["channel"], "country": row["country"]},
                "payload": {
                    "account_id": row["account_id"],
                    "merchant_id": row["merchant_id"],
                    "amount": row["amount"],
                    "currency": row["currency"],
                },
                "ts": row["transaction_timestamp"],
            }
        )
    (out_dir / "events.json").write_text(json.dumps({"events": events}, indent=2))


def _write_xml(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    """Write a simple XML document (external-data XML ingestion demo)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    root = ET.Element("merchants")
    for _, row in tables["merchants"].head(200).iterrows():
        m = ET.SubElement(root, "merchant", id=str(row["merchant_id"]))
        ET.SubElement(m, "name").text = str(row["merchant_name"])
        ET.SubElement(m, "category").text = str(row["category"])
        ET.SubElement(m, "country").text = str(row["country"])
    ET.ElementTree(root).write(out_dir / "external.xml", encoding="utf-8", xml_declaration=True)


def _write_parquet(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in (
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
    ):
        tables[name].to_parquet(
            out_dir / f"{name}.parquet",
            engine="pyarrow",
            compression="snappy",
            index=False,
        )


def _write_sftp_inbox(tables: dict[str, pd.DataFrame], out_dir: Path) -> None:
    """Split branch data into a couple of 'daily' files for the SFTP demo."""
    out_dir.mkdir(parents=True, exist_ok=True)
    branches = tables["branches"]
    half = max(1, len(branches) // 2)
    branches.iloc[:half].to_csv(out_dir / "branches_2025-01-01.csv", index=False)
    branches.iloc[half:].to_csv(out_dir / "branches_2025-01-02.csv", index=False)


def materialise(tables: dict[str, pd.DataFrame], base: Path) -> None:
    """Write all output formats under ``base`` (data/generated)."""
    _write_sqlite(tables, base / "source.sqlite")
    _write_csv(tables, base / "csv")
    _write_json(tables, base / "json")
    _write_xml(tables, base / "xml")
    _write_parquet(tables, base / "parquet")
    _write_sftp_inbox(tables, base / "sftp_inbox")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic DataForge banking data")
    parser.add_argument("--profile", default="small", help="Volume profile: small | medium | full")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic seed")
    parser.add_argument(
        "--out",
        default="data/generated",
        help="Output directory (relative to repo root)",
    )
    args = parser.parse_args(argv)

    config = load_config()
    base = config.repo_root / args.out
    cfg = build_config(args.profile, args.seed)

    _log.info(
        "generating data",
        extra={"profile": args.profile, "transactions": cfg.transactions, "seed": cfg.seed},
    )
    tables = generate_all(cfg)
    materialise(tables, base)

    summary = {name: len(df) for name, df in tables.items() if name != "customers_clean"}
    _log.info("generation complete", extra={"row_counts": summary, "out": str(base)})
    for name, count in summary.items():
        print(f"  {name:20s} {count:>10,d} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
