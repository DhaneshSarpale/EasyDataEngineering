# ==================================================================
# DataForge :: PROD environment root module
# Same modules as dev, prod naming, stricter settings (provisioned
# Kinesis, email alerts required). Keep prod in a separate AWS account
# in a real setup; here it differs only by name_prefix + tags.
# ==================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  # backend "s3" {
  #   bucket         = "dataforge-tfstate"
  #   key            = "prod/terraform.tfstate"
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
      Environment = "prod"
      ManagedBy   = "terraform"
    }
  }
}

locals {
  name_prefix = "dataforge-prod"
  tags        = { Project = "dataforge", Environment = "prod" }
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
