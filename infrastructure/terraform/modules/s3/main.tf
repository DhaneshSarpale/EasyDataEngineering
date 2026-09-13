# ------------------------------------------------------------------
# S3 module: the data-lake zones (raw/bronze/silver/gold/quarantine) +
# a glue-scripts bucket. Every bucket is:
#   - private (all public access blocked)
#   - encrypted with the environment KMS key (SSE-KMS)
#   - versioned (recover from bad writes / accidental deletes)
#   - lifecycle-tiered (raw ages to cheaper storage classes)
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  zones = ["raw", "bronze", "silver", "gold", "quarantine", "glue-scripts"]
}

resource "aws_s3_bucket" "zone" {
  for_each = toset(local.zones)
  bucket   = "${var.name_prefix}-${each.key}"
  tags     = var.tags
}

resource "aws_s3_bucket_public_access_block" "zone" {
  for_each                = aws_s3_bucket.zone
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true # reduces KMS request cost
  }
}

resource "aws_s3_bucket_versioning" "zone" {
  for_each = aws_s3_bucket.zone
  bucket   = each.value.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle: transition raw objects to cheaper tiers, expire old versions.
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.zone["raw"].id
  rule {
    id     = "tier-and-expire"
    status = "Enabled"
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    noncurrent_version_expiration { noncurrent_days = 60 }
  }
}

output "bucket_names" {
  value = { for z, b in aws_s3_bucket.zone : z => b.id }
}
output "bucket_arns" {
  value = { for z, b in aws_s3_bucket.zone : z => b.arn }
}
