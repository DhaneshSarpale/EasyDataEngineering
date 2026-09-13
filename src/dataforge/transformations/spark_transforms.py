"""PySpark transforms - distributed equivalents of the pandas transforms.

These functions operate on Spark DataFrames and are what an AWS Glue job
(or a local ``spark-submit``) would call. PySpark is imported lazily so
importing this module never fails when Spark isn't installed; the JVM is
only required when the functions are actually invoked.

The operations mirror ``pandas_transforms`` / ``window_functions`` so the
two engines stay behaviour-compatible.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # only for type hints; no runtime import
    from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str = "dataforge") -> SparkSession:
    """Create (or get) a local SparkSession. Requires the ``spark`` extra."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - optional dep
        raise RuntimeError("PySpark not installed. Install with: pip install '.[spark]'") from exc
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def filter_valid_transactions(df: DataFrame, min_amount: float = 0.0) -> DataFrame:
    """WHERE account_id IS NOT NULL AND amount > min AND status != 'REVERSED'."""
    from pyspark.sql import functions as F

    return df.where(
        F.col("account_id").isNotNull()
        & (F.col("amount") > F.lit(min_amount))
        & (F.col("status") != F.lit("REVERSED"))
    )


def cast_types(df: DataFrame, casts: dict[str, str]) -> DataFrame:
    """Cast columns. Target strings map to Spark SQL types."""
    from pyspark.sql import functions as F

    spark_types = {
        "timestamp": "timestamp",
        "date": "date",
        "decimal": "decimal(18,2)",
        "float": "double",
        "int": "int",
        "string": "string",
    }
    out = df
    for col, target in casts.items():
        if col in out.columns and target in spark_types:
            out = out.withColumn(col, F.col(col).cast(spark_types[target]))
    return out


def deduplicate(df: DataFrame, keys: list[str], order_by: str) -> DataFrame:
    """Dedup keeping the latest row per key using ROW_NUMBER()."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*keys).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def join(left: DataFrame, right: DataFrame, on: str, how: str = "inner") -> DataFrame:
    """Generic join supporting inner/left/right/full/cross/semi/anti.

    Spark ``how`` values: inner, left, right, full/outer, cross,
    left_semi, left_anti.
    """
    if how == "cross":
        return left.crossJoin(right)
    return left.join(right, on=on, how=how)


def rank_transactions(df: DataFrame) -> DataFrame:
    """RANK / DENSE_RANK of amount within account."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())
    return df.withColumn("amount_rank", F.rank().over(w)).withColumn(
        "amount_dense_rank", F.dense_rank().over(w)
    )


def running_balance(df: DataFrame) -> DataFrame:
    """SUM() OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = (
        Window.partitionBy("account_id")
        .orderBy("transaction_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df.withColumn("running_amount", F.sum("amount").over(w))


def lag_lead(df: DataFrame) -> DataFrame:
    """LAG / LEAD of amount per account (previous & next transaction)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy("account_id").orderBy("transaction_timestamp")
    return df.withColumn("prev_amount", F.lag("amount").over(w)).withColumn(
        "next_amount", F.lead("amount").over(w)
    )


def aggregate_daily(df: DataFrame) -> DataFrame:
    """daily_transaction_summary: COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT."""
    from pyspark.sql import functions as F

    return (
        df.withColumn("transaction_date", F.to_date("transaction_timestamp"))
        .groupBy("transaction_date", "currency")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.countDistinct("account_id").alias("distinct_accounts"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.min("amount").alias("min_amount"),
            F.max("amount").alias("max_amount"),
            F.sum("fraud_flag").alias("fraud_count"),
        )
    )


def to_dict_summary(df: DataFrame) -> dict[str, Any]:
    """Small helper for tests: collect a Spark DF into a row-count summary."""
    return {"rows": df.count(), "columns": len(df.columns)}
