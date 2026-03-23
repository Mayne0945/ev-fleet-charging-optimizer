output "lambda_role_arn" { value = aws_iam_role.lambda_role.arn }
output "lambda_role_id" { value = aws_iam_role.lambda_role.id }

output "silver_bucket_arn" { value = aws_s3_bucket.medallion_layers["silver"].arn }
output "gold_bucket_arn" { value = aws_s3_bucket.medallion_layers["gold"].arn }
output "gold_bucket_id" { value = aws_s3_bucket.medallion_layers["gold"].id }

output "athena_results_bucket_arn" { value = aws_s3_bucket.athena_results.arn }
output "athena_workgroup_name" { value = aws_athena_workgroup.ev_fleet.name }
output "glue_database_name" { value = aws_glue_catalog_database.ev_fleet.name }
output "lambda_role_name" { value = aws_iam_role.lambda_role.name }