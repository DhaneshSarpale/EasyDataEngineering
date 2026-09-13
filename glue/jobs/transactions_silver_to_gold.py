"""AWS Glue / PySpark job: Silver -> Gold (star schema + daily mart).

Reads cleansed silver transactions + dimensions and writes the gold-layer
star schema (dim_* + fact_transaction) and the daily aggregate mart as
partitioned Snappy Parquet, ready for Athena / Redshift Spectrum.

Runnable locally with ``spark-submit`` or as a Glue 4.0 job. Reuses the
shared Spark transforms so behaviour matches the pandas engine.
"""

from __future__ import annotations

import argparse
import sys


def _args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--silver_path", default="data/lake/silver")
    p.add_argument("--gold_path", default="data/lake/gold")
    a, _ = p.parse_known_args(argv)
    return a


def run(argv: list[str] | None = None) -> None:
    from pyspark.sql import functions as F

    from dataforge.transformations.spark_transforms import aggregate_daily, get_spark

    args = _args(argv)
    spark = get_spark("transactions_silver_to_gold")

    txn = spark.read.parquet(f"{args.silver_path}/transactions")
    accounts = spark.read.parquet(f"{args.silver_path}/accounts")
    merchants = spark.read.parquet(f"{args.silver_path}/merchants")

    # Surrogate keys via a deterministic hash (matches star_schema.py _sk).
    dim_account = accounts.withColumn(
        "account_sk", F.concat(F.lit("ACCSK_"), F.substring(F.sha1("account_id"), 1, 12))
    ).select("account_sk", "account_id", "customer_id", "account_type", "currency", "status")

    dim_merchant = merchants.withColumn(
        "merchant_sk", F.concat(F.lit("MERSK_"), F.substring(F.sha1("merchant_id"), 1, 12))
    ).select("merchant_sk", "merchant_id", "merchant_name", "category", "country")

    fact = (
        txn.join(dim_account.select("account_id", "account_sk"), "account_id", "left")
        .join(dim_merchant.select("merchant_id", "merchant_sk"), "merchant_id", "left")
        .withColumn("date_sk", F.date_format("transaction_timestamp", "yyyyMMdd"))
        .select(
            "transaction_id", "account_sk", "merchant_sk", "date_sk",
            "transaction_type", "amount", "currency", "channel", "status", "fraud_flag",
        )
    )

    daily = aggregate_daily(txn)

    for name, df in {
        "dim_account": dim_account,
        "dim_merchant": dim_merchant,
        "fact_transaction": fact,
        "daily_transaction_summary": daily,
    }.items():
        (
            df.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(f"{args.gold_path}/{name}")
        )

    print("gold layer written")
    spark.stop()


if __name__ == "__main__":
    run(sys.argv[1:])
