# Monitoring & observability

## Structured logging

Every module logs **structured JSON** to stdout
(`src/dataforge/common/logging_utils.py`) with fields like `pipeline`,
`run_id`, `records_read`, `records_written`, `records_failed`, `status`.
Glue/Lambda stdout is captured into CloudWatch Logs automatically, so
CloudWatch Logs Insights / Athena / OpenSearch can query those fields
directly. Example queries: `monitoring/cloudwatch/log_insights_queries.md`.

## Metadata / run history

The `MetadataStore` records one `pipeline_run` row per execution
(start/end, records read/written/failed, status, error) and per-pipeline
watermarks — a queryable control plane independent of the logs.

## CloudWatch metrics & alarms

`monitoring/alarms/alarms.json` + the Terraform monitoring module:

| Alarm | Trigger | Why |
| --- | --- | --- |
| glue-job-failed | failed Glue tasks ≥ 1 | job health |
| stream-lag | Kinesis iterator age > 60s | consumer under-provisioned |
| sfn-failed | Step Functions ExecutionsFailed ≥ 1 | pipeline failed |
| lambda-errors | Lambda Errors > 3 / 5min | upstream API/outage |

Alarms publish to an **SNS topic** (`dataforge-*-alerts`) with optional
email subscription.

## Dashboard

`monitoring/dashboards/pipeline_dashboard.json` (also created by the
Terraform monitoring module) shows: Glue completed vs failed tasks, Kinesis
incoming records + iterator age (lag), Lambda errors/duration, and Step
Functions succeeded/failed executions.

## What to monitor

- job failures & durations (SLA)
- records processed / failed / quarantined (data health)
- streaming lag (freshness)
- API failure rate (upstream health)
- cost anomalies (via AWS Budgets — see cost doc)

## Business KPIs (served from gold)

Total Customers, Total Accounts, Transaction Volume, Transaction Value,
Fraud Rate, Loan Portfolio, Monthly Revenue — SQL in
`sql/analytics/business_queries.sql`, connectable to QuickSight/Grafana/any
BI tool over Athena or Redshift.
