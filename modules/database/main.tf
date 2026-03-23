# 1. DynamoDB table — real-time fleet state
resource "aws_dynamodb_table" "fleet_state" {
  name         = "${var.project_name}-fleet-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "vehicle_id"

  attribute {
    name = "vehicle_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = {
    Project = var.project_name
    Layer   = "real-time-state"
  }
}

# 2. IAM — allow Lambda to read/write DynamoDB
resource "aws_iam_role_policy" "lambda_dynamodb_policy" {
  name = "${var.project_name}-lambda-dynamodb-policy"
  role = module.my_lakehouse.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:Scan",
        "dynamodb:Query",
        "dynamodb:BatchWriteItem"
      ],
      Resource = aws_dynamodb_table.fleet_state.arn
    }]
  })
}

# 3. Output
output "dynamodb_table" {
  value = aws_dynamodb_table.fleet_state.name
}