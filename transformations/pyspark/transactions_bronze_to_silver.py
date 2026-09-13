"""AWS Glue / PySpark job: transactions Bronze -> Silver.

Deployable as a Glue job (Glue 4.0, Spark 3.3+) or runnable locally with
``spark-submit``. It reads raw/bronze transactions, cleans and conforms
them, quarantines bad records, and writes partitioned Snappy Parquet to
the Silver zone.

This job reuses the same transformation logic as the pandas engine (via
``dataforge.transformations.spark_transforms``) so behaviour is consistent
across engines.

DynamicFrame vs DataFrame
-------------------------
- Use a **DynamicFrame** at the *ingestion* boundary when the schema is
  messy/evolving: it tolerates mixed types (choice columns), can resolve
  them, and integrates with the Glue Data Catalog + bookmarks.
- Convert to a **DataFrame** for the heavy transformation work: the Spark
  SQL/Catalyst optimiser, window functions, and broadcast joins live here.
"""

from __future__ import annotations

import argparse
import sys


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_path", default="data/lake/bronze/transactions")
    parser.add_argument("--silver_path", default="data/lake/silver/transactions")
    parser.add_argument("--quarantine_path", default="data/lake/quarantine/transactions")
    args, _ = parser.parse_known_args(argv)
    return args


def run(argv: list[str] | None = None) -> None:
    from pyspark.sql import functions as F

    from dataforge.transformations.spark_transforms import (
        cast_types,
        deduplicate,
        filter_valid_transactions,
        get_spark,
    )

    args = _parse_args(argv)
    spark = get_spark("transactions_bronze_to_silver")

    bronze = spark.read.parquet(args.bronze_path)

    # 1) Type conformance.
    typed = cast_types(
        bronze,
        {"amount": "decimal", "transaction_timestamp": "timestamp", "fraud_flag": "int"},
    )

    # 2) Split good vs bad records (bad -> quarantine, good -> silver).
    bad = typed.where(
        F.col("account_id").isNull()
        | (F.col("amount") <= 0)
        | (F.col("currency") == "XXX")
        | (F.col("transaction_timestamp") > F.current_timestamp())
    ).withColumn("_dq_reason", F.lit("failed_silver_rules"))

    good = filter_valid_transactions(typed)

    # 3) Deduplicate on transaction_id (keep latest by updated_at).
    good = deduplicate(good, ["transaction_id"], order_by="updated_at")

    # 4) Add partition columns for pruning.
    good = (
        good.withColumn("year", F.year("transaction_timestamp"))
        .withColumn("month", F.month("transaction_timestamp"))
        .withColumn("day", F.dayofmonth("transaction_timestamp"))
    )

    # 5) Write partitioned Snappy Parquet.
    (
        good.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(args.silver_path)
    )
    bad.write.mode("append").option("compression", "snappy").parquet(args.quarantine_path)

    print(f"silver rows written; quarantined={bad.count()}")
    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1:])
