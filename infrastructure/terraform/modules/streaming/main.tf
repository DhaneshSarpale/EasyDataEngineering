# ------------------------------------------------------------------
# Streaming module: Kinesis Data Stream + Firehose delivery to S3.
# COST WARNING: a provisioned Kinesis shard bills ~$0.015/hr each (always
# on). Use ON_DEMAND mode in dev (pay per GB) to avoid idle shard cost.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "kms_key_arn" { type = string }
variable "raw_bucket_arn" { type = string }
variable "firehose_role_arn" { type = string }
variable "stream_mode" {
  type    = string
  default = "ON_DEMAND" # dev-friendly; PROVISIONED for steady high volume
}
variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_kinesis_stream" "transactions" {
  name = "${var.name_prefix}-transactions"
  stream_mode_details { stream_mode = var.stream_mode }
  retention_period = 24
  encryption_type  = "KMS"
  kms_key_id       = var.kms_key_arn
  tags             = var.tags
}

resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = "${var.name_prefix}-to-s3"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = var.firehose_role_arn
    bucket_arn = var.raw_bucket_arn
    prefix     = "stream/transactions/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    # Buffer trade-off: bigger = cheaper Athena, higher latency.
    buffering_size     = 5
    buffering_interval = 60
    compression_format = "GZIP"
  }
  tags = var.tags
}

output "stream_name" { value = aws_kinesis_stream.transactions.name }
output "stream_arn" { value = aws_kinesis_stream.transactions.arn }
