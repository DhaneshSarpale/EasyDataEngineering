# AWS Lambda functions

Lightweight, event-driven glue between services. Each handler reuses the
`dataforge` package so the same logic is unit-testable locally.

| Function | Trigger | Purpose |
| --- | --- | --- |
| `file_processor` | S3 `ObjectCreated` | validate an uploaded file, start the Glue ingestion job |
| `api_ingestion` | EventBridge schedule | pull incremental FX rates, write raw to S3 |
| `validation` | Step Functions | validate source/schema, commit watermark |
| `notifications` | Step Functions failure path | publish alerts to SNS |

## When NOT to use Lambda

- **Long / heavy processing** — 15-min timeout, 10 GB memory cap. Use Glue
  or EMR for full-table Spark jobs.
- **Sustained high-throughput streaming** — millions of invocations get
  expensive; prefer a Kinesis consumer / Kinesis Data Analytics app.
- **Large runtimes** — a full Spark/JVM stack doesn't belong in a Lambda.
- **Stateful long sessions** — Lambda is stateless and ephemeral.

## Packaging

Each function is deployed as a zip (or container image) with the
`dataforge` package vendored in. IAM roles follow least privilege
(`infrastructure/iam/`). Secrets come from Secrets Manager, never env
literals.
