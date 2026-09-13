# Data quality

`src/dataforge/data_quality/`.

## Approach

A lightweight, dependency-free **expectation engine** expresses the same
declarative idea as Great Expectations / AWS Glue Data Quality, so the
concepts run locally and in CI with no extra infrastructure. The GE and
Glue DQ equivalents are documented for AWS MODE.

An `Expectation` is either **row-level** (returns a boolean Series; failing
rows are quarantined individually) or **table-level** (a single pass/fail,
e.g. uniqueness). Each has a `severity`: `critical` can fail the pipeline,
`warn` only logs.

## Rules (from the spec)

| Rule | Level | Severity |
| --- | --- | --- |
| `transaction_id` unique | table | critical |
| `account_id` not null | row | critical |
| `amount >= 0` | row | critical |
| `currency` in valid set | row | critical |
| `transaction_timestamp` not future | row | critical |
| `status` in valid set | row | warn |
| `customer_id` unique | table | critical |
| referential integrity (account exists) | row | critical |

Valid vocabularies come from `config/<env>.yaml` (`data_quality.valid_*`),
so they change per environment without code edits.

## Bad-data handling (detect → reject → quarantine → log → continue)

The batch pipeline splits transactions with `row_failure_mask`: good rows
flow to silver; bad rows are written to the **quarantine** zone with a
`_dq_reason` column naming the failed rule(s), and the run records
`records_failed`. Valid records are **not** blocked by a few bad ones.

The synthetic generator intentionally injects: negative amount, invalid
currency (`XXX`), future timestamp, null required field, duplicate id, and
dangling FK — so the quarantine path is always exercised.

## Reporting

`make run-dq` (`data_quality/run_checks.py`) evaluates the rule sets and
writes a JSON report (`data_quality.report_path`). In prod
(`fail_pipeline_on_error: true`) a critical failure exits non-zero so the
orchestrator's DQ gate fails the run and alerts via SNS.

## Custom checks

`data_quality/custom_checks.py`: column profiling (null %, distinct,
min/max), cross-dataset referential integrity, and freshness SLA checks.

## Failure behaviour & cost

- **Failure**: DQ never crashes the pipeline in dev; it quarantines and
  continues. In prod it can hard-fail on critical breaches.
- **Cost/scale**: row-level checks are vectorised (pandas/Spark), so cost
  scales with data volume; table-level uniqueness needs a shuffle at scale.
