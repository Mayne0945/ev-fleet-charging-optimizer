# 1. Gold Lambda function
resource "aws_lambda_function" "gold_aggregator" {
  function_name    = "${var.project_name}-gold-aggregator"
  role             = aws_iam_role.lambda_role.arn
  handler          = "gold_aggregator.handler"
  runtime          = "python3.11"
  filename         = "./build/gold_lambda.zip"
  source_code_hash = filebase64sha256("./build/gold_lambda.zip")
  timeout          = 120

  layers = ["arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:18"]

  environment {
    variables = {
      GOLD_BUCKET_NAME = module.my_lakehouse.gold_id
      ATHENA_WORKGROUP = aws_athena_workgroup.ev_fleet.name
      GLUE_DATABASE    = aws_glue_catalog_database.ev_fleet.name
    }
  }
}

# 2. EventBridge rule — every 5 minutes
resource "aws_cloudwatch_event_rule" "gold_schedule" {
  name                = "${var.project_name}-gold-schedule"
  description         = "Trigger Gold aggregation every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}

# 3. Connect EventBridge to Gold Lambda
resource "aws_cloudwatch_event_target" "gold_target" {
  rule      = aws_cloudwatch_event_rule.gold_schedule.name
  target_id = "GoldAggregator"
  arn       = aws_lambda_function.gold_aggregator.arn
}

# 4. Allow EventBridge to invoke Gold Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gold_aggregator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.gold_schedule.arn
}

# 5. IAM — Gold Lambda needs to read Silver, write Gold, run Athena
resource "aws_iam_role_policy" "gold_lambda_policy" {
  name = "${var.project_name}-gold-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:ListBucket"],
        Resource = [
          module.my_lakehouse.silver_arn,
          "${module.my_lakehouse.silver_arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = ["s3:PutObject", "s3:GetObject"],
        Resource = "${module.my_lakehouse.gold_arn}/*"
      },
      {
        Effect = "Allow",
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ],
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:GetPartitions",
          "glue:GetPartition"
        ],
        Resource = "*"
      }
    ]
  })
}

# 6. Output
output "gold_aggregator_function" {
  value = aws_lambda_function.gold_aggregator.function_name
}