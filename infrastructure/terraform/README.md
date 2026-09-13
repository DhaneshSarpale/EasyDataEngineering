# Terraform infrastructure

Modular IaC for the DataForge AWS platform. **Every resource here is
billable** — deploy only to demonstrate the AWS path, then destroy.

## Layout

```
terraform/
├── modules/
│   ├── kms/           customer-managed encryption key (rotated)
│   ├── s3/            lake zones (raw/bronze/silver/gold/quarantine) + scripts
│   ├── iam/           least-privilege roles: glue, lambda, step functions
│   ├── glue/          catalog db, bronze crawler, bronze→silver job
│   ├── streaming/     Kinesis stream (on-demand) + Firehose→S3
│   ├── orchestration/ Step Functions state machine + EventBridge schedule
│   ├── monitoring/    SNS topic, log group, alarms, dashboard
│   └── networking/    VPC, subnets, NAT, S3 endpoint, data SG
├── dev/               dev environment root (wires the modules)
└── prod/              prod environment root
```

## Usage

```bash
bash scripts/deploy.sh dev      # init + validate + plan + confirm + apply
# ... demo ...
bash scripts/destroy.sh dev     # tear everything down
```

Or directly:

```bash
cd infrastructure/terraform/dev
terraform init
terraform plan
terraform apply
terraform destroy
```

## State

Local state by default. For teams, uncomment the `backend "s3"` block in
`dev/main.tf` / `prod/main.tf` and create the state bucket + DynamoDB lock
table first.

## Security

- All buckets: private, SSE-KMS, versioned.
- Roles: least privilege, scoped to the environment's bucket + KMS ARNs.
- No credentials in code — CI/CD uses **GitHub OIDC** (see
  `infrastructure/iam/github_oidc_deploy_policy.json` and
  `.github/workflows/terraform.yml`).

## Cost drivers (mark before applying)

| Resource | Cost profile |
| --- | --- |
| S3 / KMS / Lambda / Step Functions | cents; free-tier friendly |
| Glue jobs / crawlers | ~$0.44/DPU-hr, 1-min minimum |
| Kinesis (on-demand) | per-GB; provisioned shards are always-on |
| Redshift / MWAA | **expensive, always-on** — not created by default |

See `docs/cost-optimization.md`.
