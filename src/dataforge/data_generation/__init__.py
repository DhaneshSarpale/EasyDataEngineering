"""Synthetic banking data generation (Faker-based).

Produces a relationally-consistent set of banking tables plus a
configurable fraction of intentionally-bad records used to exercise the
data-quality and quarantine paths. No real customer data is ever used.
"""

from dataforge.data_generation.generators import BankDataGenerator

__all__ = ["BankDataGenerator"]
