"""Integration tests exercising ingestion against an isolated temp source DB."""

from __future__ import annotations

import pytest

from dataforge.common.metadata import MetadataStore
from dataforge.ingestion.database.extract import (
    extract_full,
    extract_incremental_by_id,
    extract_incremental_by_timestamp,
)
from dataforge.ingestion.files.file_ingestor import FileIngestor


@pytest.mark.integration
def test_full_extract(isolated_env):
    df = extract_full("customers", config=isolated_env)
    assert len(df) == 200
    assert "customer_id" in df.columns


@pytest.mark.integration
def test_incremental_timestamp_watermark(isolated_env):
    md = MetadataStore.from_config(isolated_env)
    df1, _ = extract_incremental_by_timestamp(
        "transactions", pipeline_name="t", config=isolated_env, metadata=md
    )
    md.commit_watermark("t", "transactions")
    df2, _ = extract_incremental_by_timestamp(
        "transactions", pipeline_name="t", config=isolated_env, metadata=md
    )
    assert len(df1) == 2000
    assert len(df2) == 0  # nothing new after commit


@pytest.mark.integration
def test_incremental_by_id(isolated_env):
    md = MetadataStore.from_config(isolated_env)
    df1, _ = extract_incremental_by_id(
        "accounts", id_col="account_id", pipeline_name="a", config=isolated_env, metadata=md
    )
    md.commit_watermark("a", "accounts")
    df2, _ = extract_incremental_by_id(
        "accounts", id_col="account_id", pipeline_name="a", config=isolated_env, metadata=md
    )
    assert len(df1) == 400
    assert len(df2) == 0


@pytest.mark.integration
def test_file_ingestion_and_idempotency(isolated_env, tmp_path):
    csv = tmp_path / "branches.csv"
    csv.write_text("branch_id,city\nB1,London\nB2,Paris\n")
    ingestor = FileIngestor("branches_test", required_columns=["branch_id"], config=isolated_env)
    first = ingestor.ingest_file(csv)
    second = ingestor.ingest_file(csv)  # identical -> dedup
    assert first.status == "PROCESSED"
    assert first.rows == 2
    assert second.status == "SKIPPED_DUPLICATE"


@pytest.mark.integration
def test_file_ingestion_quarantines_bad_schema(isolated_env, tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("wrong_col\n1\n")  # missing required branch_id
    ingestor = FileIngestor("branches_test2", required_columns=["branch_id"], config=isolated_env)
    result = ingestor.ingest_file(csv)
    assert result.status == "QUARANTINED"
    assert "branch_id" in result.reason
