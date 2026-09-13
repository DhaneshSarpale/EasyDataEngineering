# Security

Production-style security principles applied throughout DataForge.

## Secrets — never hardcoded

- Resolved at runtime from **environment variables** (local/CI) or **AWS
  Secrets Manager** (`src/dataforge/common/secrets.py`). Config files hold
  only *secret names/ARNs*, never values.
- `.env` is gitignored; only `.env.example` (placeholders) is committed.
- Secret values are never logged (referenced by key name).
- Card numbers are synthetic masked suffixes only; no real PANs.

## IAM — least privilege

`infrastructure/terraform/modules/iam` creates per-service roles that trust
only their service principal and are scoped to the environment's bucket +
KMS ARNs:

- **Glue role** — read/write the lake buckets, use the CMK, catalog +
  logs. No wildcards where a concrete ARN is known.
- **Lambda role** — read lake, start Glue/SFN, write logs, use CMK.
- **Step Functions role** — invoke Lambda/Glue, publish SNS.

Reference policies: `infrastructure/iam/*.json`.

## Encryption

- **At rest**: all S3 buckets use SSE-KMS with a customer-managed key
  (`modules/kms`, rotation enabled); Kinesis stream encrypted with the same
  CMK; `bucket_key_enabled` cuts KMS cost.
- **In transit**: TLS everywhere (S3/Redshift `sslmode=require`).

## S3 bucket policies

Every bucket blocks all public access, is versioned (recover from bad
writes), and raw data tiers to STANDARD_IA → GLACIER via lifecycle rules.

## CI/CD auth — GitHub OIDC (no stored keys)

`.github/workflows/terraform.yml` assumes an AWS role via **OIDC** — GitHub
issues a short-lived token, AWS exchanges it for temporary credentials. No
long-lived `AWS_ACCESS_KEY_ID` secrets exist in the repo. The role's trust
policy is scoped to `repo:ORG/dataforge:ref:refs/heads/main`
(`infrastructure/iam/github_oidc_deploy_policy.json`).

## Networking

`infrastructure/terraform/modules/networking`: a VPC with public + private
subnets, a NAT gateway, an **S3 gateway VPC endpoint** (private subnets
reach S3 without traversing the internet), and a data security group that
allows only intra-SG traffic + egress.

How the data services communicate securely:

- **Glue** runs jobs in the VPC via a Glue Connection to reach RDS/Redshift
  privately; S3 access goes through the VPC endpoint.
- **DMS** replication instance sits in private subnets, reaching the source
  DB over the private network and writing to S3 via the endpoint.
- **Redshift** is deployed in private subnets; access via security groups
  and, for BI, a bastion / VPN / PrivateLink rather than public exposure.

## Data handling

Treat all external input (files, API, streams) as untrusted: schema is
validated, SQL uses bound parameters (identifiers whitelisted), and bad
records are quarantined rather than trusted.
