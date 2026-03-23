output "ingestion_api_url" {
  value       = module.lakehouse.api_url
  description = "The HTTP API endpoint to fire EV telemetry data into"
}

output "dynamodb_live_table" {
  value       = module.database.table_name
  description = "The DynamoDB table holding real-time fleet state"
}

output "athena_query_workgroup" {
  value       = module.lakehouse.athena_workgroup_name
  description = "The Athena workgroup for Grafana to connect to"
}