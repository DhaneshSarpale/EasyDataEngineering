variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "alert_email" {
  type        = string
  description = "Email for prod pipeline alerts (required in prod)."
}
