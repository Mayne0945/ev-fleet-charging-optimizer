variable "project_name" { type = string }

locals {
  suffix = "00cc42d3"
}

# S3 Medallion Layers — Bronze, Silver, Gold
resource "aws_s3_bucket" "medallion_layers" {
  for_each = toset(["bronze", "silver", "gold"])
  bucket   = "${var.project_name}-${each.key}-${local.suffix}"
}

# Outputs
output "bronze_id"  { value = aws_s3_bucket.medallion_layers["bronze"].id }
output "bronze_arn" { value = aws_s3_bucket.medallion_layers["bronze"].arn }
output "silver_id"  { value = aws_s3_bucket.medallion_layers["silver"].id }
output "silver_arn" { value = aws_s3_bucket.medallion_layers["silver"].arn }
output "gold_id"    { value = aws_s3_bucket.medallion_layers["gold"].id }
output "gold_arn"   { value = aws_s3_bucket.medallion_layers["gold"].arn }