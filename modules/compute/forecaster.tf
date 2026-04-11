# ─────────────────────────────────────────────────────────────
# EV Fleet Demand Forecaster Infrastructure
# Lambda container image from ECR — Prophet can't fit in a zip
# ─────────────────────────────────────────────────────────────

# 1. ECR Repository — stores the Prophet container image
resource "aws_ecr_repository" "forecaster" {
  name                 = "${var.project_name}-forecaster"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Lifecycle policy — keep only the last 3 images, auto-expire older ones
resource "aws_ecr_lifecycle_policy" "forecaster" {
  repository = aws_ecr_repository.forecaster.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 3 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 3
      }
      action = { type = "expire" }
    }]
  })
}

# 2. Lambda — container image from ECR
# image_uri is set after docker build + push
# Update via: aws lambda update-function-code --image-uri <new-uri>
resource "aws_lambda_function" "forecaster" {
  function_name = "${var.project_name}-forecaster"
  role          = var.lambda_role_arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.forecaster.repository_url}:latest"

  timeout     = 600    # 10 minutes — Prophet fit on 7 days of hourly data
  memory_size = 3008   # 3GB — Prophet + pandas are memory hungry

  environment {
    variables = {
      GOLD_BUCKET_NAME      = var.gold_bucket_id
      ATHENA_WORKGROUP      = var.athena_workgroup_name
      GLUE_DATABASE         = var.glue_database_name
      ATHENA_RESULTS_BUCKET = var.athena_results_bucket_id
      
      # The Injected ML Physics & Constraints
      PHYSICAL_MAX_KW       = "500.0"  # Change per deployment
      GRID_CAPACITY_KW      = "150.0"  # Change per deployment
      TRAINING_DAYS         = "30"     # Change per deployment

      # Explicitly map the C++ engine so the non-root user can find it
      CMDSTAN      = "/opt/cmdstan/cmdstan-2.38.0"
      STAN_BACKEND = "CMDSTANPY"
      TMPDIR       = "/tmp"
    }
  }

  depends_on = [aws_ecr_repository.forecaster]
}

# 3. EventBridge — triggers forecaster every hour
resource "aws_cloudwatch_event_rule" "forecaster_schedule" {
  name                = "${var.project_name}-forecaster-schedule"
  description         = "Trigger demand forecaster every hour"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "forecaster_target" {
  rule      = aws_cloudwatch_event_rule.forecaster_schedule.name
  target_id = "DemandForecaster"
  arn       = aws_lambda_function.forecaster.arn
}

resource "aws_lambda_permission" "forecaster_eventbridge" {
  statement_id  = "AllowEventBridgeInvokeForecaster"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.forecaster.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.forecaster_schedule.arn
}

# 4. IAM — forecaster needs Athena read + Gold S3 write
resource "aws_iam_role_policy" "forecaster_policy" {
  name = "${var.project_name}-forecaster-policy"
  role = var.lambda_role_id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
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
          "glue:GetPartitions"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:GetAuthorizationToken"
        ],
        Resource = "*"
      }
    ]
  })
}

# 5. Output
output "forecaster_function_name" {
  value = aws_lambda_function.forecaster.function_name
}

output "ecr_repository_url" {
  value = aws_ecr_repository.forecaster.repository_url
}