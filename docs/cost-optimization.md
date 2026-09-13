# Cost optimization

This is a **portfolio** project — it avoids always-on expensive services
and provides a local alternative for everything.

## Cost tiers

| Service | Cost profile | DataForge default |
| --- | --- | --- |
| S3 | ~$0.023/GB-mo + requests | cheap; lifecycle to IA/Glacier |
| KMS | $1/key-mo + $0.03/10k requests | 1 key/env; `bucket_key_enabled` |
| Lambda | free tier 1M req/mo | effectively free |
| Step Functions | ~$0.025 / 1,000 transitions | ~free |
| Athena | **$5 / TB scanned** | Parquet + partitions + column pruning |
| Glue jobs | **~$0.44 / DPU-hr**, 1-min min | 2× G.1X in dev; run on demand |
| Glue crawlers | ~$0.44 / DPU-hr | scheduled, minimal |
| Kinesis (provisioned) | ~$0.015 / shard-hr (always on) | **ON_DEMAND** in dev (pay/GB) |
| Firehose | ~$0.029 / GB | on demand |
| **Redshift** | **from ~$0.25/hr (always on)** | **not created by default** |
| **MWAA** | **~$0.49/hr smallest env** | use **local Docker Airflow** instead |
| DMS | replication instance always-on | size to change rate; stop when idle |
| OpenSearch | instance always-on | optional; not default |

## Free-tier-friendly / local alternatives

| Instead of | Use locally |
| --- | --- |
| S3 lake | `data/lake/` filesystem (same zone layout) |
| RDS source | generated `source.sqlite` |
| Redshift | SQLite warehouse (`incremental_pipeline`) / DuckDB (dbt dev) |
| Kinesis | in-memory `LocalStream` |
| MWAA | `orchestration/airflow/docker-compose.yaml` |
| Glue | pandas pipeline (`make run-local`) |

## Keeping Athena cheap

Athena bills per **TB scanned**. DataForge minimises scans with columnar
**Parquet + Snappy**, **partitioning** by `year/month(/day)` (partition
pruning), and **column pruning** in queries. Use partition projection to
avoid crawler costs.

## Guardrails

- Set an **AWS Budget** with an alert before deploying AWS MODE.
- `scripts/deploy.sh` shows the plan and requires confirmation.
- **Always** run `scripts/destroy.sh <env>` when finished.
- Resources with material cost are flagged in `infrastructure/terraform/`
  module headers and in `infrastructure/terraform/README.md`.

## Rough monthly estimate (dev, light use, destroyed between demos)

S3 + KMS + Lambda + Step Functions + a few Glue runs ≈ **a few USD**.
Leaving Redshift/MWAA/Kinesis-provisioned running would dominate the bill —
which is why they're off by default.
