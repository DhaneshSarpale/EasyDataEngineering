# ------------------------------------------------------------------
# Monitoring module: SNS alerts topic, CloudWatch log groups, alarms,
# and a dashboard. This is the observability backbone for the platform.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "glue_job_name" { type = string }
variable "alert_email" {
  type    = string
  default = ""
}
variable "tags" {
  type    = map(string)
  default = {}
}

# ---- SNS alerts topic ---------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---- Log groups (retention keeps CloudWatch cost bounded) ----------
resource "aws_cloudwatch_log_group" "pipeline" {
  name              = "/${var.name_prefix}"
  retention_in_days = 30
  tags              = var.tags
}

# ---- Alarms --------------------------------------------------------
# Glue job failure alarm.
resource "aws_cloudwatch_metric_alarm" "glue_job_failed" {
  alarm_name          = "${var.name_prefix}-glue-job-failed"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  dimensions          = { JobName = var.glue_job_name, Type = "count" }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
  tags                = var.tags
}

# ---- Dashboard -----------------------------------------------------
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.name_prefix}-dashboard"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6,
        properties = {
          title  = "Glue job DPU / task failures"
          region = "eu-west-1"
          metrics = [
            ["Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", var.glue_job_name, "Type", "count"],
            ["Glue", "glue.driver.aggregate.numFailedTasks", "JobName", var.glue_job_name, "Type", "count"]
          ]
        }
      }
    ]
  })
}

output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
output "log_group_name" { value = aws_cloudwatch_log_group.pipeline.name }
