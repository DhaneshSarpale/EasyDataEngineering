# Design decisions & tradeoffs

Each decision states what was chosen, why, the main alternatives, and the
tradeoff.

## Why S3 for the data lake?

Cheap, durable (11 nines), decoupled storage/compute, integrates with Glue/
Athena/Redshift Spectrum/EMR. **Alternatives**: HDFS (ops-heavy), a
database (couples storage+compute, costly at scale). **Tradeoff**: object
storage isn't transactional — hence the medallion layering and (future)
Iceberg/Delta for ACID.

## Why Parquet?

Columnar → read only needed columns (cheap Athena, fast Spark), embedded
schema, splittable, great compression. **Alternatives**: CSV (no schema,
row-based), Avro (row-based, better for streaming/row-by-row). **Tradeoff**:
poor for row-level random writes → use for analytical, append/overwrite
workloads.

## Why the medallion (raw/bronze/silver/gold) model?

Separation of concerns: raw is replayable truth, silver is trusted, gold is
business-shaped. **Alternative**: one big transform. **Tradeoff**: more
storage + hops vs. isolation, debuggability, and cheap reprocessing.

## Why Glue (vs EMR)?

Serverless Spark — no cluster to manage, catalog + crawlers + bookmarks
built in. **Alternative**: EMR (more control/cheaper at sustained scale but
ops-heavy). **Tradeoff**: DPU pricing + cold starts vs. zero ops.

## Why Athena?

Serverless SQL over S3, pay per TB scanned, no infra. **Alternative**:
Redshift Spectrum, Trino. **Tradeoff**: not for low-latency dashboards on
huge scans — partition/columnar to control cost, or materialise to Redshift.

## Why Redshift for the warehouse?

Fast MPP SQL for concurrent BI, DIST/SORT keys, Spectrum to S3.
**Alternatives**: Snowflake, BigQuery, Athena-only. **Tradeoff**: cluster
cost (always-on) — off by default here; use Athena/DuckDB locally.

## Why Kinesis (vs Kafka/MSK)?

Managed, serverless-ish, integrates with Firehose/Lambda/KDA.
**Alternative**: MSK/Kafka (richer ecosystem, more ops). **Tradeoff**:
shard model + AWS lock-in vs. lower operational burden.

## Why DMS for CDC?

Managed full-load + log-based CDC without touching the source app.
**Alternative**: Debezium/Kafka Connect (more flexible, self-managed).
**Tradeoff**: always-on replication instance cost.

## Why Step Functions (vs Airflow)?

Serverless, native AWS-service integration, built-in retry/catch, pay per
transition. **Alternative**: Airflow/MWAA (richer scheduling/backfills,
one pane over many pipelines). **Tradeoff**: SFN is best for bounded AWS
DAGs; Airflow for complex, cross-system scheduling (at scheduler cost).
Both are implemented here.

## Why Terraform (vs CDK/CloudFormation)?

Declarative, multi-cloud, huge module ecosystem, readable plans.
**Alternative**: CDK (real code, good for complex logic), CloudFormation
(AWS-native, verbose). **Tradeoff**: HCL learning curve vs. portability.

## Why dbt?

SQL-first modelling with tests, lineage, docs, incremental models &
snapshots (SCD2). **Alternative**: hand-written SQL + a scheduler.
**Tradeoff**: another tool to learn vs. testable, version-controlled
transforms.

## Batch vs streaming?

Both. Batch (Glue/pandas) for cost-efficient large reprocessing; streaming
(Kinesis) for real-time fraud. **Tradeoff**: streaming is lower-latency but
more complex (ordering, duplicates, late data) and can cost more per record.

## Why SCD Type 2 for customers?

Banking needs point-in-time truth ("address at time of transaction").
**Alternative**: Type 1 (overwrite, no history — cheaper/simpler).
**Tradeoff**: more rows + join complexity vs. auditability.

## Why a star schema (vs one big table / snowflake)?

Simple, fast BI joins, conformed dimensions, intuitive grain.
**Alternatives**: OBT (fast reads, storage-heavy, hard to maintain),
snowflake (normalised, more joins). **Tradeoff**: some denormalisation vs.
query simplicity and performance.

## Why a custom DQ engine (vs Great Expectations only)?

Zero-dependency, runs in CI/local instantly, teaches the concept clearly.
GE/Glue DQ equivalents are documented for AWS MODE. **Tradeoff**: fewer
built-in expectations vs. no heavy dependency and full transparency.

## Why pandas + PySpark dual engines?

pandas makes the logic testable and runnable with no JVM; PySpark is the
same logic at scale in Glue. **Tradeoff**: two implementations to keep in
sync vs. a great local DX and a real production path.
