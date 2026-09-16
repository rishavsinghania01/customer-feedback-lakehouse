resource "random_id" "suffix" {
  byte_length = 4
}

locals {
  name = "${var.project_name}-${var.environment}"
  buckets = {
    raw         = "${local.name}-raw-${random_id.suffix.hex}"
    lakehouse   = "${local.name}-lakehouse-${random_id.suffix.hex}"
    checkpoints = "${local.name}-checkpoints-${random_id.suffix.hex}"
  }
}

resource "aws_s3_bucket" "data" {
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_public_access_block" "data" {
  for_each                = aws_s3_bucket.data
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "data" {
  for_each = aws_s3_bucket.data
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_kinesis_stream" "feedback" {
  name                = "${local.name}-events"
  shard_count         = var.kinesis_shard_count
  retention_period    = 24
  encryption_type     = "KMS"
  kms_key_id          = "alias/aws/kinesis"
  shard_level_metrics = ["IncomingBytes", "IncomingRecords", "IteratorAgeMilliseconds"]
}

resource "aws_sqs_queue" "dead_letter" {
  name                      = "${local.name}-dead-letter"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

resource "aws_sqs_queue" "replay" {
  name                    = "${local.name}-replay"
  sqs_managed_sse_enabled = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_glue_catalog_database" "lakehouse" {
  name        = replace(local.name, "-", "_")
  description = "Iceberg catalog for validated and enriched customer feedback"
}

resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${local.name}"
  retention_in_days = 30
}

data "aws_iam_policy_document" "emr_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_execution" {
  name               = "${local.name}-emr-execution"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "emr_access" {
  statement {
    sid     = "DataBuckets"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = concat(
      [for bucket in aws_s3_bucket.data : bucket.arn],
      [for bucket in aws_s3_bucket.data : "${bucket.arn}/*"]
    )
  }
  statement {
    sid    = "Catalog"
    effect = "Allow"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions",
      "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
      "glue:DeletePartition"
    ]
    resources = ["*"]
  }
  statement {
    sid       = "StreamRead"
    effect    = "Allow"
    actions   = ["kinesis:DescribeStream", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards"]
    resources = [aws_kinesis_stream.feedback.arn]
  }
  statement {
    sid       = "Logs"
    effect    = "Allow"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.emr.arn}:*"]
  }
}

resource "aws_iam_role_policy" "emr_access" {
  name   = "${local.name}-data-access"
  role   = aws_iam_role.emr_execution.id
  policy = data.aws_iam_policy_document.emr_access.json
}

resource "aws_emrserverless_application" "spark" {
  name          = "${local.name}-spark"
  release_label = "emr-7.5.0"
  type          = "spark"
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}
