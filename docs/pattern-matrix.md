# Data engineering pattern matrix

Every major pattern, the technology used, a concrete example in this repo,
and where to find it.

| Pattern | Technology | Example | Location |
| --- | --- | --- | --- |
| Full extraction | Python / Glue | Customers | `ingestion/database/extract.py::extract_full` |
| Incremental (timestamp) | Python / SQL | Transactions | `extract_incremental_by_timestamp` |
| Incremental (id) | Python / SQL | Accounts | `extract_incremental_by_id` |
| CDC | DMS-style | Customers | `ingestion/cdc/cdc.py` |
| REST API | Python / Lambda | FX rates | `ingestion/api/`, `lambda/api_ingestion` |
| CSV ingestion | S3 / Glue | Branches | `ingestion/files/` |
| JSON (nested) | S3 / Glue | Events | `ingestion/files/readers.py::flatten_json` |
| XML | Glue / lxml | External merchants | `ingestion/files/readers.py::_read_xml` |
| Avro | fastavro / Glue | (format demo) | `ingestion/files/readers.py::_read_avro` |
| SFTP | Python / Transfer Family | Bank files | `ingestion/sftp/` |
| Streaming | Kinesis | Transactions | `streaming/` |
| Batch transform | pandas / Glue PySpark | Daily transactions | `transformations/`, `glue/jobs/` |
| SQL transform | Athena / Redshift | Analytics | `sql/` |
| Window functions | pandas / SQL | Running balance, ranking | `transformations/window_functions.py` |
| Aggregation | pandas / SQL | Daily summary | `aggregate_daily_transactions` |
| SCD Type 1 | Python / SQL | Customer correction | `scd_type_1`, `sql/dimensions/scd.sql` |
| SCD Type 2 | Python / SQL / dbt | Customer address history | `scd_type_2`, `dbt/snapshots/` |
| Star schema | Python / SQL | dims + facts | `star_schema.py`, `sql/facts/` |
| Data lake (medallion) | S3 | raw→bronze→silver→gold | `common/lake.py`, `pipelines/` |
| Data quality | custom / GE / Glue DQ | Transactions | `data_quality/` |
| Bad-data quarantine | S3 | Rejected transactions | `pipelines/local_batch_pipeline.py` |
| Idempotency | checksum / event id | Files & events | `common/idempotency.py` |
| Watermarking | control table | Incremental loads | `common/metadata.py` |
| Error handling | retry/backoff | API/SFTP | `common/retry.py`, `common/errors.py` |
| Orchestration | Step Functions | Daily pipeline | `orchestration/step_functions/` |
| Orchestration (alt) | Airflow | Daily DAG | `orchestration/airflow/` |
| Serverless glue | Lambda | S3 trigger, notify | `lambda/` |
| Real-time fraud | streaming rules | High-value/velocity/travel | `streaming/fraud.py` |
| Warehouse load | Redshift COPY/MERGE | fact_transaction | `sql/staging/redshift_setup.sql` |
| Query engine | Athena | External tables + CTAS | `sql/staging/athena_setup.sql` |
| Modelling | dbt | staging→marts + tests | `dbt/` |
| IaC | Terraform | All AWS resources | `infrastructure/terraform/` |
| CI/CD | GitHub Actions (OIDC) | Lint/test/plan/apply | `.github/workflows/` |
| Monitoring | CloudWatch / SNS | Alarms + dashboard | `monitoring/` |
| Security | IAM/KMS/Secrets/OIDC | Least privilege | `docs/security.md` |
| Performance | partition/prune/broadcast | Spark tuning | `transformations/pyspark/spark_optimization_example.py` |
