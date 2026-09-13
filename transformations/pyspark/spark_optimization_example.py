"""Spark optimisation: a deliberately-bad job vs an optimised rewrite.

Both compute "total posted spend per customer segment", but the naive
version triggers a large shuffle, reads unnecessary columns, and joins
before filtering. The optimised version filters early, prunes columns,
and broadcasts the small dimension.

Glossary (see docs/transformation-patterns.md for detail)
---------------------------------------------------------
- **shuffle**  : redistributing data across the cluster (expensive). Wide
                 transforms (joins, groupBy, distinct) cause shuffles.
- **partition**: a chunk of a DataFrame processed by one task.
- **executor** : a JVM process running tasks; holds cache + shuffle data.
- **driver**   : coordinates the job, builds the DAG, collects results.
- **stage**    : a set of tasks with no shuffle between them.
- **task**     : the unit of work on one partition within a stage.
"""

from __future__ import annotations


def naive(spark, transactions_path: str, customers_path: str):
    """Anti-patterns: read all columns, join then filter, no broadcast."""
    from pyspark.sql import functions as F

    txns = spark.read.parquet(transactions_path)          # reads ALL columns
    customers = spark.read.parquet(customers_path)

    joined = txns.join(customers, txns.account_id == customers.customer_id, "inner")
    result = (
        joined.where(F.col("status") == "POSTED")
        .groupBy("segment")
        .agg(F.sum("amount").alias("total_spend"))
    )
    return result


def optimised(spark, transactions_path: str, customers_path: str):
    """Filter early, prune columns, broadcast the small dimension."""
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast

    txns = (
        spark.read.parquet(transactions_path)
        .select("account_id", "amount", "status")
        .where(F.col("status") == "POSTED")                 # filter early
    )
    customers = spark.read.parquet(customers_path).select("customer_id", "segment")

    joined = txns.join(
        broadcast(customers), txns.account_id == customers.customer_id, "inner"
    )
    result = (
        joined.groupBy("segment")
        .agg(F.sum("amount").alias("total_spend"))
        .coalesce(1)
    )
    return result
