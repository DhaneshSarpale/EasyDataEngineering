# ------------------------------------------------------------------
# KMS module: a customer-managed key used to encrypt the data lake
# buckets, Kinesis stream, and secrets at rest. A single CMK per
# environment keeps key management simple while still giving audited,
# rotatable encryption (SSE-KMS) rather than SSE-S3.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_kms_key" "this" {
  description             = "${var.name_prefix} data encryption key"
  deletion_window_in_days = 7
  enable_key_rotation     = true # rotate annually (compliance-friendly)
  tags                    = var.tags
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name_prefix}"
  target_key_id = aws_kms_key.this.key_id
}

output "key_arn" { value = aws_kms_key.this.arn }
output "key_id" { value = aws_kms_key.this.key_id }
