# Kinesis Data Firehose (delivery) — reference

Firehose is the managed way to land a stream into S3 / Redshift / OpenSearch
without writing a consumer. In DataForge the streaming path fans out to all
three sinks:

```
producer ──► Kinesis Data Stream ──► Firehose ──┬─► S3 (raw stream archive, Parquet)
                                                ├─► Amazon Redshift (COPY from the S3 staging)
                                                └─► Amazon OpenSearch (real-time search / dashboards)
```

## Why Firehose

- **No consumer to run/scale** — Firehose buffers by size/time and delivers.
- **Format conversion** — can convert JSON → Parquet in flight (cheaper Athena).
- **Built-in retries + S3 backup** of failed deliveries.

## Buffering trade-off

Firehose buffers until either a **size** (e.g. 5 MB) or **time** (e.g. 60s)
threshold is hit. Bigger buffers = fewer, larger files (better for Athena)
but higher end-to-end latency. Tune to the freshness SLA.

## Terraform

The delivery stream + IAM role are provisioned in
`infrastructure/terraform/modules/streaming`. Firehose bills per GB ingested
(~$0.029/GB for the first 500 TB/mo) — see `docs/cost-optimization.md`.

## Example record transformation (JSON → typed)

Firehose can invoke a Lambda to enrich/flatten records before delivery; the
same flattening logic used in file ingestion
(`dataforge.ingestion.files.readers.flatten_json`) applies.
