output "table_name" {
  value = aws_dynamodb_table.fleet_state.name
}

output "table_arn" {
  value = aws_dynamodb_table.fleet_state.arn
}
