# Transformation patterns

DataForge implements the same transformations in four paradigms so you can
see each pattern at any scale.

| Paradigm | Location | Use |
| --- | --- | --- |
| pandas | `src/dataforge/transformations/*.py` | local, tests, small data |
| PySpark | `transformations/pyspark/`, `spark_transforms.py` | Glue / large data |
| SQL | `sql/` | Athena / Redshift |
| dbt | `dbt/` | warehouse modelling |

## Core transforms

- **Filter** (`filter_valid_transactions`) — drop nulls / non-positive
  amounts / reversed rows. Push filters *early* to cut shuffle & I/O.
- **Select / column pruning** (`select_columns`) — read only what you need.
- **Rename** (`rename_columns`) — conform naming.
- **Cast** (`cast_types`) — string→date/timestamp/decimal/int; uncoercible
  values become null (surfaced by DQ, not a crash).
- **Null handling** — `drop_null_rows`, `fill_nulls`, `conditional_default`.
- **Deduplication** (`deduplicate`) — keep the latest per key; the pandas
  equivalent of `ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1`.

## Joins

All join types are demonstrated (`pandas_transforms`, `spark_transforms`,
`sql/transformations/advanced_sql.sql`): INNER, LEFT, RIGHT, FULL, CROSS,
LEFT SEMI (`semi_join` — rows with a match), LEFT ANTI (`anti_join` — rows
*without* a match, e.g. transactions referencing a non-existent account).

## Window functions

`src/dataforge/transformations/window_functions.py` +
`sql/analytics/window_functions.sql`:

`ROW_NUMBER` (latest record), `RANK`/`DENSE_RANK` (transaction ranking),
`LAG`/`LEAD` (previous/next transaction), `SUM OVER` (running balance),
rolling 7-day sum, `PERCENT_RANK`/`NTILE` (spending percentile/quartile),
`FIRST_VALUE`/`LAST_VALUE`.

## Aggregations

`aggregate_daily_transactions` → `daily_transaction_summary`
(COUNT/SUM/AVG/MIN/MAX/COUNT DISTINCT). Business marts: monthly customer
spend, merchant revenue, fraud summary — see `sql/analytics/`.

## Advanced SQL

`sql/transformations/advanced_sql.sql`: CASE WHEN, COALESCE, NULLIF, CAST,
date & string functions, REGEX, CTEs, subqueries, UNION/UNION ALL/
INTERSECT/EXCEPT.

## SCD

- **Type 1** overwrite (`scd_type_1`).
- **Type 2** versioned history with `effective_from`/`effective_to`/
  `is_current` + surrogate key (`scd_type_2`, dbt snapshot). See
  `docs/data-model.md`.

## DynamicFrame vs DataFrame (Glue)

- **DynamicFrame** at the ingestion boundary: tolerant of schema drift
  (choice columns), integrates with the Glue Catalog + job bookmarks.
- **DataFrame** for heavy transforms: Catalyst optimiser, window functions,
  broadcast joins. Convert DynamicFrame → DataFrame → (optionally) back.

## Spark optimisation

`transformations/pyspark/spark_optimization_example.py` shows a **naive vs
optimised** job.

- **Partitioning** — write partitioned by `year/month/day` for pruning.
- **Predicate pushdown / column pruning** — filter and `select()` early so
  less data is read and shuffled.
- **Broadcast join** — ship a small dimension to every executor to avoid
  shuffling the large fact.
- **Shuffle reduction** — filter before join/groupBy; tune
  `spark.sql.shuffle.partitions`.
- **caching** — cache a DataFrame reused across actions.
- **repartition vs coalesce** — `repartition` (full shuffle, more/even
  partitions) before a wide op; `coalesce` (no shuffle) to reduce tiny
  output files.

### Glossary

- **shuffle** — redistributing data across the cluster (expensive); caused
  by wide transforms (join, groupBy, distinct).
- **partition** — a chunk of a DataFrame processed by one task.
- **executor** — a JVM worker running tasks; holds cache + shuffle data.
- **driver** — coordinates the job, builds the DAG, collects results.
- **stage** — tasks with no shuffle between them; a shuffle starts a new
  stage.
- **task** — the unit of work on one partition within a stage.

## File formats

| Format | Schema | Layout | Compression | Best for |
| --- | --- | --- | --- | --- |
| CSV | none | row | weak | interchange, tiny files |
| JSON | implicit | row | weak | nested/semi-structured, APIs |
| **Parquet** | embedded | **columnar** | Snappy/ZSTD | analytics (prune columns, cheap Athena) |
| Avro | embedded | row | Snappy/Deflate | streaming / row-by-row, schema evolution |

DataForge writes **Parquet + Snappy** to the lake: columnar storage means
Athena/Spark read only the columns a query needs, and Snappy is fast to
(de)compress and splittable.
