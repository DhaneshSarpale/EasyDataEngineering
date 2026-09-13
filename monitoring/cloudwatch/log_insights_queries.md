# CloudWatch Logs Insights queries

Because DataForge logs **structured JSON** (see
`dataforge.common.logging_utils`), Logs Insights can query fields directly.

## Failed records per run

```
fields @timestamp, pipeline, run_id, records_read, records_written, records_failed
| filter ispresent(records_failed) and records_failed > 0
| sort @timestamp desc
```

## Pipeline run outcomes (last 24h)

```
fields @timestamp, pipeline, message, status
| filter message like /run (succeeded|failed)/
| stats count(*) by status
```

## Quarantined records

```
fields @timestamp, dataset, rows
| filter message = "records quarantined"
| sort @timestamp desc
```

## Retry storms (transient errors)

```
fields @timestamp, func, attempt, error
| filter message = "retrying after error"
| stats count(*) as retries by func
| sort retries desc
```

## Fraud alerts fired

```
fields @timestamp, rule, customer_id, transaction_id
| filter message like /fraud/
| stats count(*) by rule
```
