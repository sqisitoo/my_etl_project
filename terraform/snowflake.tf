resource "snowflake_database" "raw_db" {
  name         = "RAW"
  is_transient = false
}

resource "snowflake_database" "analytic_db" {
  name         = "ANALYTICS"
  is_transient = false
}

resource "snowflake_schema" "raw_air_pollution" {
  database = snowflake_database.raw_db.name
  name     = "AIR_POLLUTION"
}


resource "snowflake_warehouse" "tf_warehouse" {
  name                      = "SNOWFLAKE_WH"
  warehouse_type            = "STANDARD"
  warehouse_size            = "XSMALL"
  max_cluster_count         = 1
  min_cluster_count         = 1
  auto_suspend              = 60
  auto_resume               = true
  enable_query_acceleration = false
  initially_suspended       = true
}

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "snowflake_role" {
  name = "snowflake-s3-role"

  assume_role_policy = jsonencode(
    length(var.snowflake_external_id) == 0 ?
    {
      Version = "2012-10-17"
      Statement = [{
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
        Condition : null
      }]
    } :
    {
      Version = "2012-10-17"
      Statement = [{
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_iam_user_arn
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      }]
    }
  )
}

resource "snowflake_storage_integration_aws" "storage_integration" {
  name                      = "aws_storage_integration"
  enabled                   = true
  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${aws_s3_bucket.datalake.id}"]
  storage_aws_role_arn      = aws_iam_role.snowflake_role.arn
}

output "snowflake_iam_user_export" {
  description = "Command to set the Snowflake IAM user ARN as an environment variable"
  value       = "export TF_VAR_snowflake_iam_user_arn='${snowflake_storage_integration_aws.storage_integration.describe_output[0].iam_user_arn}'"
}

output "snowflake_external_id_export" {
  description = "Command to set the Snowflake External ID as an environment variable"
  value       = "export TF_VAR_snowflake_external_id='${snowflake_storage_integration_aws.storage_integration.describe_output[0].external_id}'"
}