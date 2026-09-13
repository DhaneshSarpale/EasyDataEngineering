# Ingestion patterns

Every pattern below is implemented and runnable in LOCAL MODE. For each we
answer: what problem it solves, how it works, when to use it, alternatives,
pros/cons, failure behaviour, and cost/scale notes.

## 1. Full extraction

`SELECT * FROM <table>` → land the whole table.
`src/dataforge/ingestion/database/extract.py::extract_full`

- **When**: small/medium dimensions; first load; when no reliable
  change-tracking column exists.
- **Pros**: dead simple, always correct. **Cons**: re-reads everything
  every run — expensive and slow on large facts; heavy source load.
- **Alternatives**: incremental (below), CDC.
- **Failure**: idempotent by nature (full replace); safe to rerun.
- **Cost/scale**: cost grows linearly with table size every run.

## 2. Incremental by timestamp

`WHERE updated_at > :last_success_timestamp`, watermark in the control
store. `extract_incremental_by_timestamp`.

- **How**: read last watermark → pull newer rows → (load) → **commit the
  watermark only after success**. A failed run leaves the watermark
  untouched, so the next run safely reprocesses the same window.
- **When**: large tables with a reliable `updated_at`.
- **Cons**: can't capture hard DELETEs; clock skew / late updates need a
  small look-back overlap.
- **Failure**: crash before commit → reprocess window (idempotent with the
  downstream MERGE).

## 3. Incremental by id (watermarking)

`WHERE customer_id > :last_processed_id`. `extract_incremental_by_id`.

- **When**: append-only tables with a monotonically increasing key.
- **Cons**: blind to UPDATEs of already-seen rows (use timestamp or CDC).
- Ids here are fixed-width zero-padded strings, so lexical `>` matches
  numeric order.

## 4. Change Data Capture (CDC)

`src/dataforge/ingestion/cdc/cdc.py` + `docs/architecture.md`.

- **Full load** — one snapshot of current state.
- **CDC** — the continuous change stream after the snapshot.
- **Full load + CDC** — snapshot then apply changes (AWS DMS
  `full-load-and-cdc`).
- **Log-based CDC** — read the DB WAL/binlog: low source impact, captures
  every change including DELETEs. Preferred for high-volume sources.
- Events carry `operation`, `before_image`, `after_image`, `timestamp`.
  Latest state = apply events per key in time order.
- **When**: you need near-real-time replication + deletes without heavy
  full scans. **Cost**: DMS instance is always-on; size to the change rate.

## 5. REST API ingestion

`src/dataforge/ingestion/api/` (mock API + resilient client).

Handles **authentication** (Bearer token from env/Secrets Manager),
**pagination** (follows `has_next`), **retry + exponential backoff**,
**rate limiting** (429 = retryable), **timeouts**, **incremental**
(`updated_since`), and **response validation**. Retryable statuses raise
`RetryableError`; 4xx (except 429) raise `NonRetryableError` and fail fast.

- **Failure**: transient errors retried with jitter; deterministic errors
  fail immediately and are logged. Raw responses land in S3 for replay.

## 6. File ingestion

`src/dataforge/ingestion/files/`. Supports CSV/JSON/XML/Parquet/Avro.

Handles **schema detection & validation** (required columns), **corrupt
files** (→ quarantine + reason), **duplicate files** (checksum ledger →
skip), **empty files**, and **malformed records**. Nested JSON is flattened
to dotted columns during transformation.

- **Idempotent**: a file is identified by (name + sha256); re-ingesting the
  same content is a no-op.

## 7. SFTP ingestion

`src/dataforge/ingestion/sftp/`. Lifecycle: discover → download → validate
(non-empty, parseable, checksum) → idempotency check → ingest → archive →
quarantine on failure.

- **AWS MODE**: prefer **AWS Transfer Family** (managed SFTP that writes
  straight to S3, triggering a Lambda) over running a paramiko client.

## 8. Streaming ingestion

`src/dataforge/streaming/`. Kinesis-style producer/consumer with partition
keys (per-customer ordering), shards, checkpointing, retries, duplicate
handling, and late-arriving events. See `docs/architecture.md`.

## Idempotency & reliability (cross-cutting)

- **Watermarks** (`MetadataStore`) advance only after success.
- **Idempotency ledger** (`IdempotencyStore`) dedupes files (checksum) and
  events (event_id) — the same input is never processed twice.
- **Retry/backoff** for transient failures; **quarantine** for bad data;
  the pipeline continues processing valid records.
