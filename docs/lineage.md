# Data lineage

End-to-end lineage for the major datasets. Each hop names the code/SQL that
performs it, so you can trace any gold column back to its source.

## Transactions (batch)

```
source.sqlite:transactions            (generate.py)
  └─ extract_full / incremental        (ingestion/database/extract.py)
      └─ bronze/transactions           (lake.write_parquet)
          └─ DQ split                  (data_quality: run_expectations)
              ├─ quarantine/transactions   (bad rows + _dq_reason)
              └─ silver/transactions       (cast + dedup, partitioned year/month)
                  └─ fact_transaction      (star_schema.build_fact_transaction)
                  └─ daily_transaction_summary (aggregate_daily_transactions)
                      └─ Athena / Redshift / BI  (sql/analytics/*)
```

## Customers (batch + SCD2)

```
source.sqlite:customers
  └─ extract_full
      └─ bronze/customers
          └─ silver/customers
              └─ dim_customer            (star_schema.build_dim_customer)
              └─ dim_customer_scd2       (scd_type_2 / dbt snapshot)
                  └─ customer_360 / segmentation (sql/analytics)
```

## FX rates (API)

```
mock REST API /rates
  └─ ApiClient.get_all_rates (paginate + retry)
      └─ raw/api/fx_rates (lambda/api_ingestion)
          └─ enrich transactions (currency conversion)  [reference]
```

## Files (CSV/JSON/XML) + SFTP

```
data/generated/{csv,json,xml} | sftp_inbox/*.csv
  └─ FileIngestor / SftpIngestor (validate + checksum + idempotency)
      ├─ quarantine/<dataset>_rejected_files   (bad/duplicate/empty)
      └─ bronze/<dataset>                        (+ provenance columns)
```

## Streaming transactions

```
generate_event_stream
  └─ producer → Kinesis/LocalStream (partition key = customer_id)
      └─ StreamConsumer (idempotent, checkpointed)
          ├─ FraudDetector → fraud_events → OpenSearch
          └─ Firehose → raw/stream archive → Redshift
```

## CDC (customers/accounts)

```
source snapshot (full load)
  └─ CDC events (op + before/after image)      (generate_cdc_events)
      └─ reconstruct_latest_state              (apply per-key in time order)
          └─ curated latest-state table
```

For automated lineage in a real deployment, emit **OpenLineage** events
from each job to Marquez / DataHub (see `docs/*` Future improvements).
