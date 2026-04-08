# 1. Gold Lambda function
resource "aws_lambda_function" "gold_aggregator" {
  function_name    = "${var.project_name}-gold-aggregator"
  role             = var.lambda_role_arn
  handler          = "gold_aggregator.handler"
  runtime          = "python3.11"
  filename         = "${path.root}/build/gold_lambda.zip"
  source_code_hash = filebase64sha256("${path.root}/build/gold_lambda.zip")
  timeout          = 120

  # AWSSDKPandas layer removed — Gold no longer uses awswrangler or Athena

  environment {
    variables = {
      GOLD_BUCKET_NAME    = var.gold_bucket_id
      DYNAMODB_TABLE_NAME = var.dynamodb_table_name
      # ATHENA_WORKGROUP and GLUE_DATABASE removed — Gold reads from DynamoDB now
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

# 5. IAM — Gold Lambda needs DynamoDB read + Gold S3 write only
# Athena, Glue, Silver S3, and Athena results permissions removed
resource "aws_iam_role_policy" "gold_lambda_policy" {
  name = "${var.project_name}-gold-lambda-policy"
  role = var.lambda_role_id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "dynamodb:Scan",
          "dynamodb:GetItem"
        ],
        Resource = var.dynamodb_table_arn
      },
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject"
        ],
        Resource = "${var.gold_bucket_arn}/*"
      }
    ]
  })
}

# 6. Output
output "gold_aggregator_function" {
  value = aws_lambda_function.gold_aggregator.function_name
}
