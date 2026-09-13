# ------------------------------------------------------------------
# IAM module: least-privilege roles for Glue, Lambda, and Step Functions.
# Each role trusts only its service principal and is scoped to the
# environment's lake buckets + KMS key. No wildcards on resources where
# a concrete ARN is known.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "lake_bucket_arns" { type = list(string) }
variable "kms_key_arn" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}

locals {
  bucket_object_arns = [for a in var.lake_bucket_arns : "${a}/*"]
}

# ---- Glue job role -------------------------------------------------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.name_prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "glue_policy" {
  statement {
    sid       = "LakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = concat(var.lake_bucket_arns, local.bucket_object_arns)
  }
  statement {
    sid       = "UseKmsKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid       = "GlueCatalog"
    actions   = ["glue:*Table*", "glue:*Database*", "glue:*Partition*", "glue:GetCatalog*"]
    resources = ["*"] # catalog ARNs are account/region-scoped; acceptable for catalog metadata
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:/aws-glue/*"]
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "${var.name_prefix}-glue-policy"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_policy.json
}

# ---- Lambda role ---------------------------------------------------
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name_prefix}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid       = "LakeRead"
    actions   = ["s3:GetObject", "s3:ListBucket", "s3:PutObject"]
    resources = concat(var.lake_bucket_arns, local.bucket_object_arns)
  }
  statement {
    sid       = "StartGlueAndSfn"
    actions   = ["glue:StartJobRun", "states:StartExecution"]
    resources = ["*"]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:*:*:*"]
  }
  statement {
    sid       = "UseKmsKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = "${var.name_prefix}-lambda-policy"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_policy.json
}

# ---- Step Functions role ------------------------------------------
data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name               = "${var.name_prefix}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "sfn_policy" {
  statement {
    actions   = ["lambda:InvokeFunction", "glue:StartJobRun", "glue:GetJobRun", "sns:Publish"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "${var.name_prefix}-sfn-policy"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn_policy.json
}

output "glue_role_arn" { value = aws_iam_role.glue.arn }
output "lambda_role_arn" { value = aws_iam_role.lambda.arn }
output "sfn_role_arn" { value = aws_iam_role.sfn.arn }
