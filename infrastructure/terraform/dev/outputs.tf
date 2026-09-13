output "lake_buckets" {
  description = "Data-lake zone bucket names."
  value       = module.s3.bucket_names
}

output "kms_key_arn" {
  value = module.kms.key_arn
}

output "glue_database" {
  value = module.glue.database_name
}

output "state_machine_arn" {
  value = module.orchestration.state_machine_arn
}

output "alerts_topic_arn" {
  value = module.monitoring.alerts_topic_arn
}
