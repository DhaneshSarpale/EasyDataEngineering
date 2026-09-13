"""Unit tests for CDC reconstruction and streaming fraud detection."""

from __future__ import annotations

import pandas as pd
import pytest

from dataforge.ingestion.cdc.cdc import generate_cdc_events, reconstruct_latest_state
from dataforge.streaming.events import TransactionEvent
from dataforge.streaming.fraud import FraudDetector


@pytest.mark.unit
def test_cdc_reconstruction_applies_insert_update_delete():
    snapshot = pd.DataFrame({"branch_id": ["B1", "B2", "B3"], "city": ["London", "Paris", "Tokyo"]})
    events = generate_cdc_events(
        snapshot, "branch_id", n_updates=1, n_inserts=2, n_deletes=1, seed=1
    )
    latest = reconstruct_latest_state(snapshot, events, "branch_id")
    # start 3, +2 inserts, -1 delete = 4 rows.
    assert len(latest) == 3 + 2 - 1


@pytest.mark.unit
def test_fraud_high_value(monkeypatch):
    detector = FraudDetector()
    detector.high_value = 5000
    ev = TransactionEvent(
        "T1", "C1", "A1", 9000.0, "EUR", "M", "London", "2025-06-01T12:00:00+00:00"
    )
    rules = {a.rule for a in detector.evaluate(ev)}
    assert "HIGH_VALUE" in rules


@pytest.mark.unit
def test_fraud_impossible_travel():
    detector = FraudDetector()
    base = "2025-06-01T12:00:00+00:00"
    soon = "2025-06-01T12:00:30+00:00"  # 30s later, London -> Tokyo
    detector.evaluate(TransactionEvent("T1", "C1", "A1", 10.0, "GBP", "M", "London", base))
    alerts = detector.evaluate(TransactionEvent("T2", "C1", "A1", 10.0, "JPY", "M", "Tokyo", soon))
    assert "IMPOSSIBLE_TRAVEL" in {a.rule for a in alerts}


@pytest.mark.unit
def test_fraud_velocity():
    detector = FraudDetector()
    detector.velocity_window = 60
    detector.velocity_max = 3
    alerts = []
    for i in range(5):
        ts = f"2025-06-01T12:00:{i*5:02d}+00:00"
        alerts = detector.evaluate(
            TransactionEvent(f"T{i}", "C1", "A1", 10.0, "EUR", "M", "London", ts)
        )
    assert "VELOCITY" in {a.rule for a in alerts}
