"""Real-time fraud detection rules (demonstration only, not a real system).

Stateful per-customer rules evaluated as each event arrives:

- **HIGH_VALUE**        amount >= configured threshold.
- **VELOCITY**          more than N transactions within a rolling time window.
- **IMPOSSIBLE_TRAVEL** implied travel speed between consecutive cities
                        exceeds a physically-plausible maximum.

The detector keeps a small in-memory history per customer. In a real
system this state would live in a low-latency store (e.g. Kinesis Data
Analytics / Flink state, or DynamoDB / ElastiCache) so it survives across
shards and restarts. Thresholds come from ``config.streaming.fraud``.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime

from dataforge.common.config import Config, load_config
from dataforge.data_generation.generators import CITY_GEO
from dataforge.streaming.events import TransactionEvent


@dataclass
class FraudAlert:
    """A fired fraud rule for a given event."""

    transaction_id: str
    customer_id: str
    rule: str
    severity: str
    detail: str


def _haversine_km(city_a: str, city_b: str) -> float | None:
    """Great-circle distance (km) between two known cities, or None."""
    if city_a not in CITY_GEO or city_b not in CITY_GEO:
        return None
    lat1, lon1, _ = CITY_GEO[city_a]
    lat2, lon2, _ = CITY_GEO[city_b]
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class FraudDetector:
    """Evaluate fraud rules over a per-customer event history."""

    def __init__(self, config: Config | None = None) -> None:
        config = config or load_config()
        fraud = config.get("streaming.fraud", {})
        self.high_value = float(fraud.get("high_value_threshold", 5000))
        self.velocity_window = float(fraud.get("velocity_window_seconds", 60))
        self.velocity_max = int(fraud.get("velocity_max_txns", 3))
        self.impossible_kmh = float(fraud.get("impossible_travel_kmh", 900))
        # Per-customer recent timestamps (velocity) and last (ts, city).
        self._recent: dict[str, deque[datetime]] = defaultdict(deque)
        self._last_location: dict[str, tuple[datetime, str]] = {}

    def evaluate(self, event: TransactionEvent) -> list[FraudAlert]:
        """Return any fraud alerts triggered by this event."""
        alerts: list[FraudAlert] = []
        ts = datetime.fromisoformat(event.timestamp)
        cust = event.customer_id

        # --- HIGH_VALUE ---
        if event.amount >= self.high_value:
            alerts.append(
                FraudAlert(
                    event.transaction_id,
                    cust,
                    "HIGH_VALUE",
                    "HIGH",
                    f"amount {event.amount} >= {self.high_value}",
                )
            )

        # --- VELOCITY (rolling window count) ---
        window = self._recent[cust]
        window.append(ts)
        cutoff = ts.timestamp() - self.velocity_window
        while window and window[0].timestamp() < cutoff:
            window.popleft()
        if len(window) > self.velocity_max:
            alerts.append(
                FraudAlert(
                    event.transaction_id,
                    cust,
                    "VELOCITY",
                    "MEDIUM",
                    f"{len(window)} txns within {self.velocity_window}s",
                )
            )

        # --- IMPOSSIBLE_TRAVEL ---
        last = self._last_location.get(cust)
        if last is not None:
            last_ts, last_city = last
            dist = _haversine_km(last_city, event.city)
            dt_hours = abs((ts - last_ts).total_seconds()) / 3600.0
            if dist is not None and dt_hours > 0:
                speed = dist / dt_hours
                if speed > self.impossible_kmh:
                    alerts.append(
                        FraudAlert(
                            event.transaction_id,
                            cust,
                            "IMPOSSIBLE_TRAVEL",
                            "HIGH",
                            f"{dist:.0f}km in {dt_hours*60:.1f}min " f"=> {speed:.0f}km/h",
                        )
                    )
        # Only advance last-location for forward-in-time events (ignore late
        # arrivals so they don't corrupt the travel baseline).
        if last is None or ts >= last[0]:
            self._last_location[cust] = (ts, event.city)

        return alerts
