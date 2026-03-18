# ─────────────────────────────────────────────────────────────
# DataDawgz — Kinesis Ingestion & Bronze Layer
# Replaces: API Gateway + Dummy Lambda
# ─────────────────────────────────────────────────────────────

# 1. Kinesis Data Stream — real-time transaction ingestion
resource "aws_kinesis_stream" "transactions" {
  name             = "${var.project_name}-transactions"
  shard_count      = 2
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "IteratorAgeMilliseconds",
    "WriteProvisionedThroughputExceeded",
  ]

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Layer = "ingestion"
  }
}

# 2. Dead-Letter Queue — failed records land here, never silently dropped
resource "aws_sqs_queue" "consumer_dlq" {
  name                      = "${var.project_name}-consumer-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue_policy" "consumer_dlq" {
  queue_url = aws_sqs_queue.consumer_dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.consumer_dlq.arn
    }]
  })
}

# 3. Lambda — Kinesis consumer → writes raw NDJSON to Bronze S3
resource "aws_lambda_function" "ingestor" {
  function_name    = "${var.project_name}-ingestor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256
  filename         = "${path.module}/dummy_lambda.zip"
  source_code_hash = filebase64sha256("${path.module}/dummy_lambda.zip")

  environment {
    variables = {
      BRONZE_BUCKET = aws_s3_bucket.medallion_layers["bronze"].id
      LOG_LEVEL     = "INFO"
    }
  }

  tracing_config {
    mode = "Active"
  }
}

# 4. CloudWatch Log Group — explicit retention
resource "aws_cloudwatch_log_group" "ingestor" {
  name              = "/aws/lambda/${var.project_name}-ingestor"
  retention_in_days = 14
}

# 5. Kinesis → Lambda trigger
resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn               = aws_kinesis_stream.transactions.arn
  function_name                  = aws_lambda_function.ingestor.arn
  starting_position              = "LATEST"
  batch_size                     = 100
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.consumer_dlq.arn
    }
  }
}

# 6. Output the stream name for the producer script
output "kinesis_stream_name" {
  value = aws_kinesis_stream.transactions.name
}