# Transformations

DataForge implements transformations in four paradigms. The importable,
tested Python logic lives in the package (`src/dataforge/transformations/`);
the directories here hold the deployable/reference artefacts.

| Paradigm | Location | Runs where |
| --- | --- | --- |
| **pandas** | `src/dataforge/transformations/pandas_transforms.py`, `window_functions.py`, `scd.py`, `star_schema.py` | LOCAL MODE, tests, small data |
| **PySpark** | `transformations/pyspark/*.py` + `src/dataforge/transformations/spark_transforms.py` | AWS Glue / `spark-submit`, large data |
| **SQL** | `sql/` (transformations, dimensions, facts, incremental, analytics) | Athena / Redshift |
| **dbt** | `dbt/` | Redshift / warehouse via dbt |

Every operation demonstrated in pandas has a Spark and/or SQL equivalent so
the same behaviour can run at any scale. See
[`docs/transformation-patterns.md`](../docs/transformation-patterns.md).

- `pyspark/transactions_bronze_to_silver.py` — deployable Glue job: clean,
  dedup, quarantine bad rows, write partitioned Snappy Parquet.
- `pyspark/spark_optimization_example.py` — naive vs optimised job
  (filter early, prune columns, broadcast join, coalesce).
