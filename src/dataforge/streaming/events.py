"""Streaming transaction events + a generator that injects fraud patterns.

Each event mirrors the JSON envelope a payment gateway would emit::

    {
      "transaction_id": "...",
      "customer_id": "...",
      "account_id": "...",
      "amount": 1500,
      "currency": "EUR",
      "merchant": "Amazon",
      "city": "London",
      "timestamp": "..."
    }

The generator seeds a few deliberate fraud scenarios so the downstream
detector has something to catch:

- **high value**        one very large amount
- **velocity**          several transactions for one customer within seconds
- **impossible travel** two transactions in distant cities seconds apart
- **duplicates**        the same event id emitted twice (idempotency test)
- **late arrival**      an event whose timestamp is older than its neighbours
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from dataforge.data_generation.generators import CITY_GEO

CITIES = list(CITY_GEO.keys())


@dataclass
class TransactionEvent:
    """A single streaming transaction event."""

    transaction_id: str
    customer_id: str
    account_id: str
    amount: float
    currency: str
    merchant: str
    city: str
    timestamp: str  # ISO-8601

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def partition_key(self) -> str:
        """Kinesis partition key.

        Using ``customer_id`` routes all of a customer's events to the same
        shard, preserving per-customer ordering - essential for the velocity
        and impossible-travel rules to see events in order.
        """
        return self.customer_id


def generate_event_stream(
    n: int = 200, *, seed: int = 3, inject_fraud: bool = True
) -> Iterator[TransactionEvent]:
    """Yield ``n`` transaction events with a few embedded fraud scenarios."""
    import numpy as np

    rng = np.random.default_rng(seed)
    base = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    currencies = ["USD", "EUR", "GBP", "INR"]
    merchants = ["Amazon", "Tesco", "Uber", "Apple", "Shell", "Netflix"]

    # Assign each base customer a stable "home" city so the normal stream
    # does not spuriously trigger the impossible-travel rule; only the
    # injected fraud scenarios below cross cities.
    home_city: dict[str, str] = {}
    for i in range(n):
        cust = f"CUST{rng.integers(1, 500):08d}"
        city = home_city.setdefault(cust, CITIES[int(rng.integers(0, len(CITIES)))])
        yield TransactionEvent(
            transaction_id=f"STXN{i:08d}",
            customer_id=cust,
            account_id=f"ACC{rng.integers(1, 1000):08d}",
            amount=round(float(rng.gamma(2.0, 120.0)) + 1, 2),
            currency=currencies[int(rng.integers(0, len(currencies)))],
            merchant=merchants[int(rng.integers(0, len(merchants)))],
            city=city,
            timestamp=(base + timedelta(seconds=i * 5)).isoformat(),
        )

    if not inject_fraud:
        return

    fraud_cust = "CUST00000042"
    t = base + timedelta(hours=1)

    # High-value transaction.
    yield TransactionEvent(
        "STXNFRAUD01",
        fraud_cust,
        "ACC00000042",
        25000.0,
        "EUR",
        "LuxuryCars",
        "London",
        t.isoformat(),
    )
    # Velocity: 4 rapid transactions within the velocity window.
    for j in range(4):
        yield TransactionEvent(
            f"STXNVEL{j:02d}",
            fraud_cust,
            "ACC00000042",
            900.0 + j,
            "EUR",
            "Online",
            "London",
            (t + timedelta(seconds=10 * (j + 1))).isoformat(),
        )
    # Impossible travel: London then Tokyo ~30s apart.
    yield TransactionEvent(
        "STXNTRV01",
        fraud_cust,
        "ACC00000042",
        400.0,
        "GBP",
        "Cafe",
        "London",
        (t + timedelta(minutes=5)).isoformat(),
    )
    yield TransactionEvent(
        "STXNTRV02",
        fraud_cust,
        "ACC00000042",
        500.0,
        "JPY",
        "Sushi",
        "Tokyo",
        (t + timedelta(minutes=5, seconds=30)).isoformat(),
    )
    # Duplicate event (same id) - consumer must drop it.
    yield TransactionEvent(
        "STXNTRV02",
        fraud_cust,
        "ACC00000042",
        500.0,
        "JPY",
        "Sushi",
        "Tokyo",
        (t + timedelta(minutes=5, seconds=30)).isoformat(),
    )
    # Late-arriving event (earlier timestamp than its neighbours).
    yield TransactionEvent(
        "STXNLATE01",
        fraud_cust,
        "ACC00000042",
        50.0,
        "GBP",
        "Kiosk",
        "London",
        (t - timedelta(minutes=30)).isoformat(),
    )
