resource "random_string" "bucket_suffix" {
  length  = 12
  special = false
  upper   = false
}

resource "aws_s3_bucket" "datalake" {
  force_destroy = false

  tags = {
    Name = "DataLake"
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true

}