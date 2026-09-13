# ==================================================================
# DataForge :: DEV environment root module
# Wires the reusable modules together for the dev account. Apply with:
#   cd infrastructure/terraform/dev && terraform init && terraform apply
# See scripts/deploy.sh dev.  Destroy with scripts/destroy.sh dev.
#
# COST: S3/KMS/Lambda/Step Functions are cheap/free-tier friendly.
# Glue jobs, Kinesis (provisioned), and Redshift are the cost drivers and
# are OFF or minimal here. See docs/cost-optimization.md.
# ==================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # Remote state (uncomment + create the bucket/table first):
  # backend "s3" {
  #   bucket         = "dataforge-tfstate"
  #   key            = "dev/terraform.tfstate"
  #   region         = "eu-west-1"
  #   dynamodb_table = "dataforge-tflock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project     = "dataforge"
      Environment = "dev"
      ManagedBy   = "terraform"
    }
  }
}

locals {
  name_prefix = "dataforge-dev"
  tags        = { Project = "dataforge", Environment = "dev" }
}

module "kms" {
  source      = "../modules/kms"
  name_prefix = local.name_prefix
  tags        = local.tags
}

module "s3" {
  source      = "../modules/s3"
  name_prefix = local.name_prefix
  kms_key_arn = module.kms.key_arn
  tags        = local.tags
}

module "iam" {
  source           = "../modules/iam"
  name_prefix      = local.name_prefix
  lake_bucket_arns = values(module.s3.bucket_arns)
  kms_key_arn      = module.kms.key_arn
  tags             = local.tags
}

module "glue" {
  source         = "../modules/glue"
  name_prefix    = local.name_prefix
  glue_role_arn  = module.iam.glue_role_arn
  scripts_bucket = module.s3.bucket_names["glue-scripts"]
  bronze_bucket  = module.s3.bucket_names["bronze"]
  silver_bucket  = module.s3.bucket_names["silver"]
  tags           = local.tags
}

module "monitoring" {
  source        = "../modules/monitoring"
  name_prefix   = local.name_prefix
  glue_job_name = module.glue.bronze_to_silver_job
  alert_email   = var.alert_email
  tags          = local.tags
}

module "orchestration" {
  source         = "../modules/orchestration"
  name_prefix    = local.name_prefix
  sfn_role_arn   = module.iam.sfn_role_arn
  asl_definition = file("${path.module}/../../../orchestration/step_functions/daily_pipeline.asl.json")
  tags           = local.tags
}
