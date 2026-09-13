"""Relationally-consistent synthetic banking data generator.

Design notes
------------
- Deterministic: a fixed ``seed`` makes runs reproducible (important for
  tests and for a stable portfolio demo).
- Referential integrity: accounts reference existing customers, cards and
  loans reference accounts, transactions reference accounts + merchants,
  etc. This lets join / SCD / fact-dimension examples be meaningful.
- Vectorised: large tables (transactions) are built with numpy for speed,
  so the ``full`` 2M-row profile completes in reasonable time. Faker is
  used for the human-readable attributes on the smaller tables.
- SCD-ready: customers carry an ``updated_at`` and a subset get a second
  "changed" version (new address/city) so SCD Type 2 has something to do.
- Bad data: a ``bad_data_ratio`` fraction of transactions/customers are
  corrupted in well-defined ways (negative amount, invalid currency,
  future timestamp, missing customer, duplicate id, null required field).

Each generator method returns a ``pandas.DataFrame``. The orchestration
(volume, output formats, source DB) lives in ``generate.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from faker import Faker

# Reference vocabularies kept small & explicit so DQ rules can validate them.
CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD", "CHF"]
COUNTRIES = ["US", "GB", "DE", "FR", "IN", "JP", "CA", "AU", "CH", "SG"]
CHANNELS = ["POS", "ATM", "ONLINE", "MOBILE", "WIRE", "SFTP"]
TXN_TYPES = ["PURCHASE", "WITHDRAWAL", "DEPOSIT", "TRANSFER", "REFUND", "FEE"]
TXN_STATUS = ["PENDING", "POSTED", "DECLINED", "REVERSED"]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX"]
LOAN_TYPES = ["PERSONAL", "MORTGAGE", "AUTO", "STUDENT", "BUSINESS"]
MERCHANT_CATEGORIES = [
    "GROCERY",
    "TRAVEL",
    "ELECTRONICS",
    "RESTAURANT",
    "FUEL",
    "UTILITIES",
    "ENTERTAINMENT",
    "HEALTHCARE",
    "RETAIL",
    "SERVICES",
]

# Approximate lat/lon for a few cities, used by the impossible-travel
# fraud rule and by branch/merchant geo attributes.
CITY_GEO = {
    "New York": (40.71, -74.01, "US"),
    "London": (51.51, -0.13, "GB"),
    "Berlin": (52.52, 13.40, "DE"),
    "Paris": (48.86, 2.35, "FR"),
    "Mumbai": (19.08, 72.88, "IN"),
    "Tokyo": (35.68, 139.69, "JP"),
    "Toronto": (43.65, -79.38, "CA"),
    "Sydney": (-33.87, 151.21, "AU"),
    "Zurich": (47.37, 8.54, "CH"),
    "Singapore": (1.35, 103.82, "SG"),
}
CITIES = list(CITY_GEO.keys())


@dataclass
class GeneratorConfig:
    """Volume + behaviour knobs for a single generation run."""

    customers: int = 1000
    accounts: int = 2000
    transactions: int = 20000
    cards: int = 1000
    loans: int = 500
    merchants: int = 300
    branches: int = 50
    employees: int = 400
    payments: int = 5000
    fraud_events: int = 200
    bad_data_ratio: float = 0.02
    seed: int = 42
    start_date: datetime = field(default_factory=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc))
    end_date: datetime = field(default_factory=lambda: datetime(2025, 12, 31, tzinfo=timezone.utc))


class BankDataGenerator:
    """Generate a consistent set of DataForge Bank tables."""

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.faker = Faker()
        Faker.seed(cfg.seed)
        self.rng = np.random.default_rng(cfg.seed)
        self._customer_ids: np.ndarray | None = None
        self._account_ids: np.ndarray | None = None
        self._merchant_ids: np.ndarray | None = None
        self._branch_ids: np.ndarray | None = None

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _random_timestamps(self, n: int) -> pd.Series:
        span = (self.cfg.end_date - self.cfg.start_date).total_seconds()
        offsets = self.rng.uniform(0, span, size=n)
        base = self.cfg.start_date
        return pd.Series([base + timedelta(seconds=float(o)) for o in offsets])

    def _ids(self, prefix: str, n: int, width: int = 8) -> np.ndarray:
        return np.array([f"{prefix}{i:0{width}d}" for i in range(1, n + 1)])

    # ------------------------------------------------------------------
    # dimension-like tables
    # ------------------------------------------------------------------
    def gen_branches(self) -> pd.DataFrame:
        n = self.cfg.branches
        self._branch_ids = self._ids("BR", n, 5)
        cities = self.rng.choice(CITIES, size=n)
        rows = {
            "branch_id": self._branch_ids,
            "branch_name": [f"{c} Branch {i}" for i, c in enumerate(cities, 1)],
            "city": cities,
            "country": [CITY_GEO[c][2] for c in cities],
            "latitude": [CITY_GEO[c][0] for c in cities],
            "longitude": [CITY_GEO[c][1] for c in cities],
            "opened_date": self._random_timestamps(n).dt.date.astype(str),
        }
        return pd.DataFrame(rows)

    def gen_employees(self) -> pd.DataFrame:
        n = self.cfg.employees
        assert self._branch_ids is not None, "generate branches first"
        return pd.DataFrame(
            {
                "employee_id": self._ids("EMP", n, 6),
                "first_name": [self.faker.first_name() for _ in range(n)],
                "last_name": [self.faker.last_name() for _ in range(n)],
                "branch_id": self.rng.choice(self._branch_ids, size=n),
                "role": self.rng.choice(["TELLER", "MANAGER", "ANALYST", "ADVISOR"], size=n),
                "hire_date": self._random_timestamps(n).dt.date.astype(str),
            }
        )

    def gen_merchants(self) -> pd.DataFrame:
        n = self.cfg.merchants
        self._merchant_ids = self._ids("MER", n, 6)
        cities = self.rng.choice(CITIES, size=n)
        return pd.DataFrame(
            {
                "merchant_id": self._merchant_ids,
                "merchant_name": [self.faker.company() for _ in range(n)],
                "category": self.rng.choice(MERCHANT_CATEGORIES, size=n),
                "city": cities,
                "country": [CITY_GEO[c][2] for c in cities],
            }
        )

    def gen_customers(self) -> pd.DataFrame:
        n = self.cfg.customers
        self._customer_ids = self._ids("CUST", n)
        cities = self.rng.choice(CITIES, size=n)
        created = self._random_timestamps(n)
        df = pd.DataFrame(
            {
                "customer_id": self._customer_ids,
                "first_name": [self.faker.first_name() for _ in range(n)],
                "last_name": [self.faker.last_name() for _ in range(n)],
                "email": [self.faker.unique.email() for _ in range(n)],
                "phone": [self.faker.phone_number() for _ in range(n)],
                "address": [self.faker.street_address() for _ in range(n)],
                "city": cities,
                "country": [CITY_GEO[c][2] for c in cities],
                "segment": self.rng.choice(
                    ["RETAIL", "PREMIUM", "PRIVATE", "BUSINESS"],
                    size=n,
                    p=[0.6, 0.25, 0.05, 0.10],
                ),
                "created_at": created,
                "updated_at": created,
            }
        )
        return df

    # ------------------------------------------------------------------
    # transactional tables
    # ------------------------------------------------------------------
    def gen_accounts(self) -> pd.DataFrame:
        n = self.cfg.accounts
        assert self._customer_ids is not None, "generate customers first"
        self._account_ids = self._ids("ACC", n)
        opened = self._random_timestamps(n)
        return pd.DataFrame(
            {
                "account_id": self._account_ids,
                "customer_id": self.rng.choice(self._customer_ids, size=n),
                "account_type": self.rng.choice(ACCOUNT_TYPES, size=n),
                "currency": self.rng.choice(CURRENCIES, size=n),
                "balance": np.round(self.rng.uniform(0, 250000, size=n), 2),
                "status": self.rng.choice(
                    ["ACTIVE", "DORMANT", "CLOSED"], size=n, p=[0.8, 0.15, 0.05]
                ),
                "opened_at": opened,
                "updated_at": opened,
            }
        )

    def gen_cards(self) -> pd.DataFrame:
        n = self.cfg.cards
        assert self._account_ids is not None, "generate accounts first"
        issued = self._random_timestamps(n)
        return pd.DataFrame(
            {
                "card_id": self._ids("CARD", n),
                "account_id": self.rng.choice(self._account_ids, size=n),
                "card_type": self.rng.choice(CARD_TYPES, size=n),
                "card_last4": [f"{self.rng.integers(0, 9999):04d}" for _ in range(n)],
                "credit_limit": np.round(self.rng.uniform(1000, 50000, size=n), 2),
                "status": self.rng.choice(
                    ["ACTIVE", "BLOCKED", "EXPIRED"], size=n, p=[0.85, 0.05, 0.10]
                ),
                "issued_at": issued,
                "expires_at": (issued + pd.to_timedelta(1460, unit="D")),
            }
        )

    def gen_loans(self) -> pd.DataFrame:
        n = self.cfg.loans
        assert self._account_ids is not None, "generate accounts first"
        principal = np.round(self.rng.uniform(1000, 500000, size=n), 2)
        return pd.DataFrame(
            {
                "loan_id": self._ids("LOAN", n),
                "account_id": self.rng.choice(self._account_ids, size=n),
                "loan_type": self.rng.choice(LOAN_TYPES, size=n),
                "principal": principal,
                "interest_rate": np.round(self.rng.uniform(2.5, 18.0, size=n), 2),
                "term_months": self.rng.choice([12, 24, 36, 60, 120, 240, 360], size=n),
                "outstanding": np.round(principal * self.rng.uniform(0, 1, size=n), 2),
                "status": self.rng.choice(
                    ["ACTIVE", "PAID_OFF", "DEFAULT"], size=n, p=[0.75, 0.20, 0.05]
                ),
                "opened_at": self._random_timestamps(n),
            }
        )

    def gen_transactions(self) -> pd.DataFrame:
        """Build the large transactions fact table (vectorised)."""
        n = self.cfg.transactions
        assert self._account_ids is not None and self._merchant_ids is not None
        ts = self._random_timestamps(n)
        df = pd.DataFrame(
            {
                "transaction_id": self._ids("TXN", n, 10),
                "account_id": self.rng.choice(self._account_ids, size=n),
                "merchant_id": self.rng.choice(self._merchant_ids, size=n),
                "transaction_type": self.rng.choice(TXN_TYPES, size=n),
                "amount": np.round(self.rng.gamma(2.0, 120.0, size=n) + 1, 2),
                "currency": self.rng.choice(CURRENCIES, size=n),
                "channel": self.rng.choice(CHANNELS, size=n),
                "country": self.rng.choice(COUNTRIES, size=n),
                "status": self.rng.choice(TXN_STATUS, size=n, p=[0.10, 0.82, 0.06, 0.02]),
                "fraud_flag": self.rng.choice([0, 1], size=n, p=[0.985, 0.015]),
                "transaction_timestamp": ts,
                "updated_at": ts,
            }
        )
        return df

    def gen_payments(self) -> pd.DataFrame:
        n = self.cfg.payments
        assert self._account_ids is not None
        ts = self._random_timestamps(n)
        return pd.DataFrame(
            {
                "payment_id": self._ids("PAY", n),
                "account_id": self.rng.choice(self._account_ids, size=n),
                "payee": [self.faker.company() for _ in range(n)],
                "amount": np.round(self.rng.uniform(10, 20000, size=n), 2),
                "currency": self.rng.choice(CURRENCIES, size=n),
                "method": self.rng.choice(["ACH", "WIRE", "CARD", "STANDING_ORDER"], size=n),
                "status": self.rng.choice(
                    ["COMPLETED", "PENDING", "FAILED"], size=n, p=[0.9, 0.07, 0.03]
                ),
                "payment_timestamp": ts,
            }
        )

    def gen_fraud_events(self, transactions: pd.DataFrame) -> pd.DataFrame:
        """Fraud events reference a subset of transactions flagged as fraud."""
        flagged = transactions[transactions["fraud_flag"] == 1]
        n = min(self.cfg.fraud_events, len(flagged)) if len(flagged) else 0
        if n == 0:
            return pd.DataFrame(
                columns=[
                    "fraud_event_id",
                    "transaction_id",
                    "rule",
                    "severity",
                    "detected_at",
                    "resolved",
                ]
            )
        sample = flagged.sample(n=n, random_state=self.cfg.seed)
        return pd.DataFrame(
            {
                "fraud_event_id": self._ids("FRD", n),
                "transaction_id": sample["transaction_id"].to_numpy(),
                "rule": self.rng.choice(
                    ["HIGH_VALUE", "VELOCITY", "IMPOSSIBLE_TRAVEL", "BLACKLIST"],
                    size=n,
                ),
                "severity": self.rng.choice(["LOW", "MEDIUM", "HIGH"], size=n),
                "detected_at": sample["transaction_timestamp"].to_numpy(),
                "resolved": self.rng.choice([0, 1], size=n, p=[0.7, 0.3]),
            }
        )

    # ------------------------------------------------------------------
    # SCD: produce "changed" customer versions
    # ------------------------------------------------------------------
    def gen_customer_changes(self, customers: pd.DataFrame, frac: float = 0.1) -> pd.DataFrame:
        """Return a set of updated customer rows (new address/city/updated_at).

        These represent source-system UPDATEs that SCD Type 2 must track as
        new versioned rows.
        """
        if customers.empty:
            return customers.copy()
        n_changed = max(1, int(len(customers) * frac))
        changed = customers.sample(n=n_changed, random_state=self.cfg.seed + 1).copy()
        new_cities = self.rng.choice(CITIES, size=n_changed)
        changed["city"] = new_cities
        changed["country"] = [CITY_GEO[c][2] for c in new_cities]
        changed["address"] = [self.faker.street_address() for _ in range(n_changed)]
        bump = pd.to_timedelta(self.rng.integers(1, 180, size=n_changed), unit="D")
        changed["updated_at"] = pd.to_datetime(changed["updated_at"]) + bump
        return changed

    # ------------------------------------------------------------------
    # bad data injection
    # ------------------------------------------------------------------
    def inject_bad_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Corrupt a fraction of transactions in well-defined ways."""
        ratio = self.cfg.bad_data_ratio
        if ratio <= 0 or df.empty:
            return df
        df = df.copy()
        n = len(df)
        n_bad = max(6, int(n * ratio))
        idx = self.rng.choice(n, size=min(n_bad, n), replace=False)
        buckets = np.array_split(idx, 6)

        # 1) negative amount
        df.loc[df.index[buckets[0]], "amount"] = -df.loc[df.index[buckets[0]], "amount"]
        # 2) invalid currency
        df.loc[df.index[buckets[1]], "currency"] = "XXX"
        # 3) future timestamp
        future = self.cfg.end_date + timedelta(days=400)
        df.loc[df.index[buckets[2]], "transaction_timestamp"] = future
        # 4) null required account_id
        df.loc[df.index[buckets[3]], "account_id"] = None
        # 5) duplicate transaction_id
        for i in buckets[4]:
            src = (i + 1) % n
            df.iloc[i, df.columns.get_loc("transaction_id")] = df.iloc[
                src, df.columns.get_loc("transaction_id")
            ]
        # 6) dangling account reference
        df.loc[df.index[buckets[5]], "account_id"] = "ACC99999999"
        return df

    def inject_bad_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Null out emails and blank names for a small fraction of customers."""
        ratio = self.cfg.bad_data_ratio
        if ratio <= 0 or df.empty:
            return df
        df = df.copy()
        n = len(df)
        n_bad = max(2, int(n * ratio))
        idx = self.rng.choice(n, size=min(n_bad, n), replace=False)
        half = np.array_split(idx, 2)
        df.loc[df.index[half[0]], "email"] = None
        df.loc[df.index[half[1]], "first_name"] = ""
        return df
