#!/usr/bin/env bash
# ==================================================================
# Tear down DataForge AWS infrastructure to stop incurring charges.
# Usage: bash scripts/destroy.sh <env>   (env = dev | prod)
# ==================================================================
set -euo pipefail

ENV="${1:-dev}"
if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "Usage: $0 <dev|prod>" >&2
  exit 1
fi

TF_DIR="$(cd "$(dirname "$0")/.." && pwd)/infrastructure/terraform/${ENV}"
echo ">> Destroying DataForge [${ENV}] from ${TF_DIR}"

command -v terraform >/dev/null 2>&1 || { echo "terraform not installed" >&2; exit 1; }

cd "$TF_DIR"
terraform init -input=false
terraform destroy -auto-approve
echo ">> Destroy complete. Verify in the AWS console that no resources remain."
