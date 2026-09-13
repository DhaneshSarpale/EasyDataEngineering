# ------------------------------------------------------------------
# Orchestration module: the Step Functions state machine + an
# EventBridge schedule that triggers it daily. The ASL definition is
# read from orchestration/step_functions/daily_pipeline.asl.json.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "sfn_role_arn" { type = string }
variable "asl_definition" { type = string } # rendered ASL JSON
variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_sfn_state_machine" "daily" {
  name       = "${var.name_prefix}-daily"
  role_arn   = var.sfn_role_arn
  definition = var.asl_definition
  type       = "STANDARD"
  tags       = var.tags
}

# EventBridge role to start the state machine.
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy" "scheduler" {
  name = "${var.name_prefix}-scheduler-policy"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.daily.arn]
    }]
  })
}

resource "aws_scheduler_schedule" "daily" {
  name                         = "${var.name_prefix}-daily-batch"
  schedule_expression          = "cron(0 3 * * ? *)"
  schedule_expression_timezone = "UTC"
  flexible_time_window { mode = "OFF" }
  target {
    arn      = aws_sfn_state_machine.daily.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ table = "transactions" })
  }
}

output "state_machine_arn" { value = aws_sfn_state_machine.daily.arn }
