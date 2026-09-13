output "lake_buckets" {
  value = module.s3.bucket_names
}

output "state_machine_arn" {
  value = module.orchestration.state_machine_arn
}

output "alerts_topic_arn" {
  value = module.monitoring.alerts_topic_arn
}
