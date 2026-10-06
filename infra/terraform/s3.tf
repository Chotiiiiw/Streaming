data "aws_caller_identity" "current" {}

locals {
  artifact_bucket_name = lower(
    "${var.project_name}-${var.environment}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  )

  data_bucket_name = lower(
    "${var.project_name}-${var.environment}-data-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  )

  flink_jar_path = abspath(var.flink_jar_path)
}

resource "aws_s3_bucket" "data" {
  bucket = local.data_bucket_name

  tags = {
    Name = local.data_bucket_name
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket" "artifacts" {
  bucket = local.artifact_bucket_name

  # This is an ephemeral learning environment. Allow terraform destroy
  # to remove the bucket even when it contains application artifacts.
  force_destroy = true

  tags = {
    Name = local.artifact_bucket_name
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  depends_on = [
    aws_s3_bucket_versioning.artifacts
  ]

  rule {
    id     = "expire-old-artifact-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

resource "aws_s3_object" "flink_application" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "applications/transaction-routing-job.jar"

  source      = local.flink_jar_path
  source_hash = fileexists(local.flink_jar_path) ? filemd5(local.flink_jar_path) : null

  server_side_encryption = "AES256"

  depends_on = [
    aws_s3_bucket_server_side_encryption_configuration.artifacts,
    aws_s3_bucket_versioning.artifacts
  ]

  lifecycle {
    precondition {
      condition     = fileexists(local.flink_jar_path)
      error_message = "Build the Flink JAR first with: cd ../../flink-app && mvn clean package"
    }
  }
}
