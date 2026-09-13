variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "AWS region for the dev environment."
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional email for SNS pipeline alerts (leave blank to skip)."
}
