output "bucket_names" {
  description = "S3 buckets used by the lakehouse."
  value       = { for key, bucket in aws_s3_bucket.data : key => bucket.bucket }
}

output "kinesis_stream_name" {
  value = aws_kinesis_stream.feedback.name
}

output "glue_database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}

output "emr_serverless_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_execution_role_arn" {
  value = aws_iam_role.emr_execution.arn
}

output "replay_queue_url" {
  value = aws_sqs_queue.replay.url
}
