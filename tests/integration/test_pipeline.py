"""Integration test: the local batch + incremental pipelines end-to-end."""

from __future__ import annotations

import pytest

from dataforge.pipelines import incremental_pipeline, local_batch_pipeline


@pytest.mark.integration
def test_local_batch_pipeline_builds_gold(isolated_env):
    summary = local_batch_pipeline.run(config=isolated_env)
    # Bronze has all extracted rows; silver <= bronze (bad rows quarantined).
    assert summary["bronze_transactions"] == 2000
    assert summary["silver_transactions"] <= summary["bronze_transactions"]
    assert summary["fact_transaction"] == summary["silver_transactions"]
    assert summary["dim_customer"] == 200
    assert summary["daily_transaction_summary"] > 0


@pytest.mark.integration
def test_incremental_pipeline_is_idempotent(isolated_env):
    r1 = incremental_pipeline.run(config=isolated_env)
    r2 = incremental_pipeline.run(config=isolated_env)
    assert r1["extracted"] == 2000
    assert r1["merged"] > 0
    # Second run: watermark advanced -> nothing new to process.
    assert r2["extracted"] == 0
    assert r2["merged"] == 0
