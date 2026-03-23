# 1. Gold Lambda function
resource "aws_lambda_function" "gold_aggregator" {
  function_name    = "${var.project_name}-gold-aggregator"
  role             = var.lambda_role_arn
  handler          = "gold_aggregator.handler"
  runtime          = "python3.11"
  filename         = "${path.root}/build/gold_lambda.zip"
  source_code_hash = filebase64sha256("${path.root}/build/gold_lambda.zip")
  timeout          = 120

  layers = ["arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:18"]

  environment {
    variables = {
      GOLD_BUCKET_NAME = var.gold_bucket_id
      ATHENA_WORKGROUP = var.athena_workgroup_name
      GLUE_DATABASE    = var.glue_database_name
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
  role = var.lambda_role_id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:ListBucket"],
        Resource = [
          var.silver_bucket_arn,
          "${var.silver_bucket_arn}/*"
        ]
      },
      {
        Effect = "Allow",
        Action = ["s3:PutObject", "s3:GetObject"],
        Resource = "${var.gold_bucket_arn}/*"
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
          var.athena_results_bucket_arn,
          "${var.athena_results_bucket_arn}/*"
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