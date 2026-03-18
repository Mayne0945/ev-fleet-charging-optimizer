# 1. IAM Policy — Bronze read/write, Silver read/write
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "${var.project_name}-lambda-s3-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:PutObject"],
        Resource = "${aws_s3_bucket.medallion_layers["bronze"].arn}/*"
      },
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject"],
        Resource = "${aws_s3_bucket.medallion_layers["bronze"].arn}/*"
      },
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        Resource = "${aws_s3_bucket.medallion_layers["silver"].arn}/*"
      },
      {
        Effect   = "Allow",
        Action   = ["s3:ListBucket"],
        Resource = [
          aws_s3_bucket.medallion_layers["bronze"].arn,
          aws_s3_bucket.medallion_layers["silver"].arn
        ]
      }
    ]
  })
}

# 2. The ingestor Lambda (API Gateway → Bronze)
resource "aws_lambda_function" "ingestor" {
  function_name    = "${var.project_name}-ingestor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "python3.11"
  filename         = "${path.module}/dummy_lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/dummy_lambda.zip")

  environment {
    variables = {
      BRONZE_BUCKET_NAME = aws_s3_bucket.medallion_layers["bronze"].id
    }
  }
}

# 3. The transformer Lambda (Bronze → Silver Parquet)
resource "aws_lambda_function" "transformer" {
  function_name    = "${var.project_name}-transformer"
  role             = aws_iam_role.lambda_role.arn
  handler          = "silver_transform.handler"
  runtime          = "python3.11"
  filename         = "${path.module}/silver_lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/silver_lambda.zip")
  layers           = ["arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:18"]
  timeout          = 60

  environment {
    variables = {
      SILVER_BUCKET_NAME = aws_s3_bucket.medallion_layers["silver"].id
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.fleet_state.name
    }
  }
}

# 4. The Front Door: HTTP API Gateway
resource "aws_apigatewayv2_api" "telemetry_api" {
  name          = "${var.project_name}-telemetry-api"
  protocol_type = "HTTP"
}

# 5. The Connector: Link API to ingestor Lambda
resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.telemetry_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.ingestor.invoke_arn
}

# 6. The Route: Listen for POST requests from the vehicles
resource "aws_apigatewayv2_route" "post_telemetry" {
  api_id    = aws_apigatewayv2_api.telemetry_api.id
  route_key = "POST /telemetry"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

# 7. The Stage: Auto-deploy changes
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.telemetry_api.id
  name        = "$default"
  auto_deploy = true
}

# 8. Give API Gateway permission to trigger the ingestor Lambda
resource "aws_lambda_permission" "api_gw" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestor.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.telemetry_api.execution_arn}/*/*"
}

# 9. Allow Bronze bucket to trigger the transformer Lambda
resource "aws_lambda_permission" "allow_bronze_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.medallion_layers["bronze"].arn
}

# 10. S3 event trigger: Bronze → transformer
resource "aws_s3_bucket_notification" "bronze_trigger" {
  bucket = aws_s3_bucket.medallion_layers["bronze"].id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transformer.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_bronze_trigger]
}

# 11. Print out the public URL
output "api_endpoint" {
  value = "${aws_apigatewayv2_api.telemetry_api.api_endpoint}/telemetry"
}