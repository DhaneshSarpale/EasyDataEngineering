# ------------------------------------------------------------------
# Glue module: Data Catalog database, crawlers, and jobs.
# COST WARNING: Glue jobs bill per DPU-hour (~$0.44) with a 1-min minimum;
# crawlers bill similarly. Keep worker counts low in dev.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "glue_role_arn" { type = string }
variable "scripts_bucket" { type = string }
variable "bronze_bucket" { type = string }
variable "silver_bucket" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_glue_catalog_database" "this" {
  name = replace("${var.name_prefix}_catalog", "-", "_")
}

resource "aws_glue_crawler" "bronze" {
  name          = "${var.name_prefix}-bronze-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.this.name
  s3_target {
    path = "s3://${var.bronze_bucket}/"
  }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
  tags = var.tags
}

resource "aws_glue_job" "bronze_to_silver" {
  name              = "transactions_bronze_to_silver"
  role_arn          = var.glue_role_arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2 # keep small in dev to limit cost
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/jobs/transactions_bronze_to_silver.py"
    python_version  = "3"
  }
  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--bronze_path"                      = "s3://${var.bronze_bucket}/transactions/"
    "--silver_path"                      = "s3://${var.silver_bucket}/transactions/"
  }
  tags = var.tags
}

output "database_name" { value = aws_glue_catalog_database.this.name }
output "bronze_to_silver_job" { value = aws_glue_job.bronze_to_silver.name }
