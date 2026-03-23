# 1. IAM — Bronze read/write, Silver read/write
resource "aws_iam_role_policy" "lambda_s3_access" {
  name = "${var.project_name}-lambda-s3-policy"
  role = aws_iam_role.lambda_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:PutObject", "s3:GetObject"],
        Resource = "${module.my_lakehouse.bronze_arn}/*"
      },
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject"],
        Resource = "${module.my_lakehouse.silver_arn}/*"
      },
      {
        Effect = "Allow",
        Action = ["s3:ListBucket", "s3:GetBucketLocation"],
        Resource = [
          module.my_lakehouse.bronze_arn,
          module.my_lakehouse.silver_arn
        ]
      }
    ]
  })
}

# 2. IAM — SQS read
resource "aws_iam_role_policy" "lambda_sqs_access" {
  name = "${var.project_name}-lambda-sqs-policy"
  role = aws_iam_role.lambda_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      Resource = aws_sqs_queue.telemetry_queue.arn
    }]
  })
}

# 4. Ingestor Lambda (SQS → Bronze)
resource "aws_lambda_function" "ingestor" {
  function_name    = "${var.project_name}-ingestor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "python3.11"
  filename         = "./modules/dummy_lambda.zip"
  source_code_hash = filebase64sha256("./modules/dummy_lambda.zip")

  environment {
    variables = {
      BRONZE_BUCKET_NAME = module.my_lakehouse.bronze_id
    }
  }
}

# 5. Transformer Lambda (Bronze → Silver + DynamoDB)
resource "aws_lambda_function" "transformer" {
  function_name    = "${var.project_name}-transformer"
  role             = aws_iam_role.lambda_role.arn
  handler          = "silver_transform.handler"
  runtime          = "python3.11"
  filename         = "./modules/silver_lambda.zip"
  source_code_hash = filebase64sha256("./modules/silver_lambda.zip")
  timeout          = 60

  layers = ["arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python311:18"]

  environment {
    variables = {
      SILVER_BUCKET_NAME  = module.my_lakehouse.silver_id
      DYNAMODB_TABLE_NAME = aws_dynamodb_table.fleet_state.name
    }
  }
}

# 6. API Gateway (backup ingestion path)
resource "aws_apigatewayv2_api" "telemetry_api" {
  name          = "${var.project_name}-telemetry-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "lambda_integration" {
  api_id           = aws_apigatewayv2_api.telemetry_api.id
  integration_type = "AWS_PROXY"
  integration_uri  = aws_lambda_function.ingestor.invoke_arn
}

resource "aws_apigatewayv2_route" "post_telemetry" {
  api_id    = aws_apigatewayv2_api.telemetry_api.id
  route_key = "POST /telemetry"
  target    = "integrations/${aws_apigatewayv2_integration.lambda_integration.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.telemetry_api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api_gw" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestor.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.telemetry_api.execution_arn}/*/*"
}

# 7. S3 event trigger: Bronze → Transformer
resource "aws_lambda_permission" "allow_bronze_trigger" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.my_lakehouse.bronze_arn
}

resource "aws_s3_bucket_notification" "bronze_trigger" {
  bucket = module.my_lakehouse.bronze_id

  lambda_function {
    lambda_function_arn = aws_lambda_function.transformer.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_bronze_trigger]
}

# 8. Outputs
output "api_url" {
  value = "${aws_apigatewayv2_api.telemetry_api.api_endpoint}/telemetry"
}

output "my_buckets" {
  value = [
    module.my_lakehouse.bronze_id,
    module.my_lakehouse.silver_id,
  ]
}