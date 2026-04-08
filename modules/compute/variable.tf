variable "project_name" { type = string }
variable "dynamodb_table_name" { type = string }
variable "dynamodb_table_arn" { type = string }

# IAM
variable "lambda_role_arn" { type = string }
variable "lambda_role_id" { type = string }

# S3
variable "silver_bucket_arn" { type = string }
variable "gold_bucket_arn" { type = string }
variable "gold_bucket_id" { type = string }
variable "athena_results_bucket_arn" { type = string }
variable "athena_results_bucket_id" { type = string }

# Analytics
variable "athena_workgroup_name" { type = string }
variable "glue_database_name" { type = string }
