"""Transformation library: pandas + PySpark, SCD, aggregations, windows.

The pandas engine (``pandas_transforms``) is the default for LOCAL MODE and
tests - no JVM required. The PySpark engine (``spark_transforms``) mirrors
the same operations for distributed / AWS Glue execution and is imported
lazily so the package works even when PySpark isn't installed.
"""

from dataforge.transformations.pandas_transforms import (
    aggregate_daily_transactions,
    cast_types,
    deduplicate,
    fill_nulls,
    filter_valid_transactions,
    join_transactions_customers,
    rename_columns,
    select_columns,
)
from dataforge.transformations.scd import scd_type_1, scd_type_2

__all__ = [
    "filter_valid_transactions",
    "select_columns",
    "rename_columns",
    "cast_types",
    "fill_nulls",
    "deduplicate",
    "join_transactions_customers",
    "aggregate_daily_transactions",
    "scd_type_1",
    "scd_type_2",
]
