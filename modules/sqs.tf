# 1. Dead Letter Queue — catches poison pill messages
resource "aws_sqs_queue" "telemetry_dlq" {
  name                      = "${var.project_name}-telemetry-dlq"
  message_retention_seconds = 1209600  # 14 days
}

# 2. Main telemetry queue
resource "aws_sqs_queue" "telemetry_queue" {
  name                       = "${var.project_name}-telemetry-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 86400   # 24 hours

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.telemetry_dlq.arn
    maxReceiveCount     = 3  # After 3 failures → DLQ
  })
}

# 3. Allow API Gateway to send messages to SQS
resource "aws_sqs_queue_policy" "telemetry_queue_policy" {
  queue_url = aws_sqs_queue.telemetry_queue.url

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "apigateway.amazonaws.com" },
      Action    = "sqs:SendMessage",
      Resource  = aws_sqs_queue.telemetry_queue.arn
    }]
  })
}

# 4. Allow Lambda to read from SQS
resource "aws_iam_role_policy" "lambda_sqs_policy" {
  name = "${var.project_name}-lambda-sqs-policy"
  role = aws_iam_role.lambda_role.id

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

# 5. SQS triggers ingestor Lambda
resource "aws_lambda_event_source_mapping" "sqs_to_ingestor" {
  event_source_arn = aws_sqs_queue.telemetry_queue.arn
  function_name    = aws_lambda_function.ingestor.arn
  batch_size       = 10
  enabled          = true
}

# 6. Outputs
output "sqs_queue_url" {
  value = aws_sqs_queue.telemetry_queue.url
}

output "sqs_dlq_url" {
  value = aws_sqs_queue.telemetry_dlq.url
}