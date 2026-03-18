# 1. Glue Database — the container for all EV fleet tables
resource "aws_glue_catalog_database" "ev_fleet" {
  name        = "${var.project_name}-database"
  description = "EV Fleet telemetry data lake — Medallion architecture"
}

# 2. Glue Table — maps Silver Parquet to a queryable schema
resource "aws_glue_catalog_table" "ev_telemetry" {
  name          = "ev_telemetry"
  database_name = aws_glue_catalog_database.ev_fleet.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"       = "parquet"
    "parquet.compression"  = "SNAPPY"
    "projection.enabled"   = "false"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.medallion_layers["silver"].bucket}/ev_telemetry/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "parquet-serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    # Non-partition columns
    columns {
      name    = "vehicle_id"
      type    = "string"
    }
    columns {
      name    = "timestamp"
      type    = "string"
    }
    columns {
      name    = "state_of_charge"
      type    = "double"
    }
    columns {
      name    = "status"
      type    = "string"
    }
    columns {
      name    = "charger_connected"
      type    = "boolean"
    }
    columns {
      name    = "charger_type"
      type    = "string"
    }
    columns {
      name    = "charger_kw"
      type    = "double"
    }
    columns {
      name    = "battery_temp_c"
      type    = "double"
    }
    columns {
      name    = "speed_kmh"
      type    = "double"
    }
    columns {
      name    = "odometer_km"
      type    = "double"
    }
    columns {
      name    = "next_departure"
      type    = "string"
    }
    columns {
      name    = "time_to_departure_min"
      type    = "double"
    }
    columns {
      name    = "tariff_period"
      type    = "string"
    }
    columns {
      name    = "lat"
      type    = "double"
    }
    columns {
      name    = "long"
      type    = "double"
    }
    columns {
      name    = "processed_at"
      type    = "string"
    }
    columns {
      name    = "source_key"
      type    = "string"
    }
    columns {
      name    = "validation_errors"
      type    = "string"
    }
    columns {
      name    = "data_quality"
      type    = "string"
    }
  }

  # Partition columns — must match Silver S3 structure
  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
}

# 3. IAM — allow Glue to read Silver S3
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject", "s3:ListBucket"],
        Resource = [
          aws_s3_bucket.medallion_layers["silver"].arn,
          "${aws_s3_bucket.medallion_layers["silver"].arn}/*"
        ]
      }
    ]
  })
}

# 4. Athena results bucket — Athena needs somewhere to write query results
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-athena-results-${random_id.suffix.hex}"
  force_destroy = true
}

# 5. Athena workgroup — isolates our queries and points to results bucket
resource "aws_athena_workgroup" "ev_fleet" {
  name        = "${var.project_name}-workgroup"
  description = "EV Fleet query workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }
}

# 6. Output the workgroup name for reference
output "athena_workgroup" {
  value = aws_athena_workgroup.ev_fleet.name
}

output "glue_database" {
  value = aws_glue_catalog_database.ev_fleet.name
}