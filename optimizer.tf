# 1. Optimizer Lambda
resource "aws_lambda_function" "optimizer" {
  function_name    = "${var.project_name}-optimizer"
  role             = aws_iam_role.lambda_role.arn
  handler          = "optimizer.handler"
  runtime          = "python3.11"
  filename         = "./modules/optimizer_lambda.zip"
  source_code_hash = filebase64sha256("./modules/optimizer_lambda.zip")
  timeout          = 60

  environment {
    variables = {
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.fleet_state.name
      GOLD_BUCKET_NAME    = module.my_lakehouse.gold_id
    }
  }
}

# 2. EventBridge — every 5 minutes
resource "aws_cloudwatch_event_rule" "optimizer_schedule" {
  name                = "${var.project_name}-optimizer-schedule"
  description         = "Trigger optimizer every 5 minutes"
  schedule_expression = "rate(5 minutes)"
}

# 3. Connect EventBridge to optimizer
resource "aws_cloudwatch_event_target" "optimizer_target" {
  rule      = aws_cloudwatch_event_rule.optimizer_schedule.name
  target_id = "OptimizerEngine"
  arn       = aws_lambda_function.optimizer.arn
}

# 4. Allow EventBridge to invoke optimizer
resource "aws_lambda_permission" "allow_eventbridge_optimizer" {
  statement_id  = "AllowEventBridgeInvokeOptimizer"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.optimizer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.optimizer_schedule.arn
}

# 5. Output
output "optimizer_function" {
  value = aws_lambda_function.optimizer.function_name
}