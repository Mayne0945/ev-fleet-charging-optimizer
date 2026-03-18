# reusable blueprint for a 3-layer Data Lakehouse
variable "project_name" {
  type = string
}

resource "aws_s3_bucket" "medallion_layers" {
  for_each = toset(["bronze", "silver", "gold"])
  bucket = "${var.project_name}-${each.key}-${random_id.suffix.hex}"
}

resource "random_id" "suffix" {
  byte_length = 4
}

output "bucket_ids" {
  value = [for b in aws_s3_bucket.medallion_layers : b.id]
}