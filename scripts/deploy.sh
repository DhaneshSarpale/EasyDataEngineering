#!/usr/bin/env bash
# ==================================================================
# Deploy DataForge AWS infrastructure with Terraform.
# Usage: bash scripts/deploy.sh <env>   (env = dev | prod)
#
# WARNING: This creates billable AWS resources. Review the plan before
# confirming. Run scripts/destroy.sh <env> afterwards to avoid charges.
# ==================================================================
set -euo pipefail

ENV="${1:-dev}"
if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "Usage: $0 <dev|prod>" >&2
  exit 1
fi

TF_DIR="$(cd "$(dirname "$0")/.." && pwd)/infrastructure/terraform/${ENV}"
echo ">> Deploying DataForge [${ENV}] from ${TF_DIR}"

command -v terraform >/dev/null 2>&1 || { echo "terraform not installed" >&2; exit 1; }

cd "$TF_DIR"
terraform init -input=false
terraform validate
terraform plan -out=tfplan
echo ""
read -r -p ">> Apply this plan? This will create billable resources. [y/N] " ans
if [[ "${ans:-N}" =~ ^[Yy]$ ]]; then
  terraform apply -input=false tfplan
  echo ">> Deploy complete. Remember to run: bash scripts/destroy.sh ${ENV}"
else
  echo ">> Aborted. No changes applied."
fi
