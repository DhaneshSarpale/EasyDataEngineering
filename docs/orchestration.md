# Orchestration

Two orchestrators are provided; they run the same lifecycle.

## AWS Step Functions (primary)

`orchestration/step_functions/daily_pipeline.asl.json`.

```
START
 → ValidateSource   (Lambda)          retry x2, backoff 2.0
 → Extract          (Glue .sync)      retry on ConcurrentRuns
 → ValidateSchema   (Lambda)
 → Transform        (Glue .sync)      bronze → silver
 → DataQuality      (Glue .sync)
 → DataQualityGate  (Choice)          dq_passed == false → CaptureError
 → Load             (Glue .sync)      silver → gold
 → UpdateMetadata   (Lambda)          commit watermark
 → SUCCESS

Failure (Catch on every task):
 CaptureError (normalise) → Notify (SNS) → FAIL → CloudWatch alarm
```

**Why Step Functions**: visual, auditable state machine with built-in
retries/catch/timeouts (no bespoke retry code), `.sync` Glue integration
(block until a job finishes), and declarative branching (the DQ gate).
Triggered daily by EventBridge (`orchestration/schedules/`). Cost is
trivial (~$0.025 / 1,000 state transitions); the Glue jobs it invokes are
the real cost.

## AWS Lambda

`lambda/` — lightweight, event-driven glue:

| Function | Trigger | Purpose |
| --- | --- | --- |
| file_processor | S3 ObjectCreated | validate upload, start Glue |
| api_ingestion | EventBridge | pull FX rates → S3 |
| validation | Step Functions | validate source/schema, commit watermark |
| notifications | SFN failure | publish to SNS |

**When NOT to use Lambda**: long/heavy processing (15-min, 10 GB caps →
use Glue/EMR); sustained high-throughput streaming (millions of invokes →
Kinesis consumer/KDA); large Spark/JVM runtimes; long stateful sessions.

## Apache Airflow (alternative)

`orchestration/airflow/dags/dataforge_daily_dag.py`: DAG
`extract >> transform >> data_quality >> load` using PythonOperators,
XComs (row counts), per-task retries with exponential backoff, and a daily
schedule. Run locally with `orchestration/airflow/docker-compose.yaml`
(free) rather than paying for Amazon MWAA.

## Choosing

- **Step Functions** — serverless, tight AWS-service integration, pay per
  transition, great for a bounded DAG of AWS tasks.
- **Airflow/MWAA** — richer scheduling, backfills, a single pane across
  many diverse pipelines; always-on scheduler cost.
