# Step Functions orchestration

`daily_pipeline.asl.json` is the Amazon States Language (ASL) definition of
the DataForge daily batch pipeline.

```
START
  └─ ValidateSource   (Lambda)         retry x2, backoff 2.0
  └─ Extract          (Glue job .sync) retry on ConcurrentRuns
  └─ ValidateSchema   (Lambda)
  └─ Transform        (Glue job .sync) bronze -> silver
  └─ DataQuality      (Glue job .sync)
  └─ DataQualityGate  (Choice)         dq_passed == false -> CaptureError
  └─ Load             (Glue job .sync) silver -> gold
  └─ UpdateMetadata   (Lambda)
  └─ SUCCESS

Failure path (Catch on every Task):
  CaptureError (Pass, normalise) -> Notify (SNS publish) -> Fail
```

## Why Step Functions (vs cron/Lambda chains)

- **Visual, auditable** state machine with built-in retries, catch, and
  timeouts — no bespoke retry code.
- **`.sync` integration** blocks until a Glue job finishes and surfaces its
  result, so steps are naturally sequential.
- **Choice** states express branching (the DQ gate) declaratively.
- Execution history is retained for debugging and compliance.

## Deploy

Provisioned by Terraform (`infrastructure/terraform/modules/orchestration`).
Trigger daily via an EventBridge schedule (see `../schedules/`).

## Cost

Step Functions **Standard** bills per state transition (~$0.025 / 1,000).
A daily run is a handful of transitions — effectively free. The Glue jobs
it invokes are the real cost driver; see `docs/cost-optimization.md`.
