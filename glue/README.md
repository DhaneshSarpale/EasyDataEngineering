# AWS Glue

DataForge uses Glue for three things:

1. **Data Catalog** — a central metastore of table schemas over the S3
   lake, queried by Athena and Redshift Spectrum.
2. **Crawlers** — infer schema + partitions of the raw/bronze zones and
   register them in the catalog.
3. **Jobs** — serverless PySpark ETL (bronze→silver→gold).

## DynamicFrame vs DataFrame

- **DynamicFrame**: Glue's self-describing structure. Use at the ingestion
  boundary — it tolerates schema drift (choice columns), resolves types,
  and integrates with the catalog and job **bookmarks** (Glue's built-in
  incremental state).
- **DataFrame**: standard Spark. Convert to it for the heavy transforms
  (Catalyst optimiser, window functions, broadcast joins).

## Files

- `jobs/` — deployable job scripts (see also
  `transformations/pyspark/transactions_bronze_to_silver.py`).
- `crawlers/crawler_config.json` — crawler definitions.
- `workflows/daily_workflow.json` — a Glue Workflow chaining crawler→jobs.

## Cost note

Glue jobs bill per **DPU-hour** (~$0.44/DPU-hr) with a 1-minute minimum.
Crawlers bill similarly. For the portfolio, prefer the LOCAL MODE pandas
pipeline; only run Glue when demonstrating the AWS path, and destroy after.
