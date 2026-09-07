output "artifact_bucket_name" {
  description = "S3 bucket containing the Managed Flink application artifact."
  value       = aws_s3_bucket.artifacts.id
}

output "flink_artifact_key" {
  description = "S3 object key for the deployable Flink JAR."
  value       = aws_s3_object.flink_application.key
}

output "flink_application_name" {
  description = "Name of the Managed Service for Apache Flink application."
  value       = aws_kinesisanalyticsv2_application.flink.name
}

output "flink_application_status" {
  description = "Current status of the Managed Flink application."
  value       = aws_kinesisanalyticsv2_application.flink.status
}

output "msk_cluster_arn" {
  description = "ARN of the MSK Serverless cluster."
  value       = aws_msk_serverless_cluster.main.arn
}

output "msk_bootstrap_brokers_sasl_iam" {
  description = "Private IAM-authenticated bootstrap brokers for MSK Serverless."
  value       = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
}

output "flink_log_group_name" {
  description = "CloudWatch log group for the Managed Flink application."
  value       = aws_cloudwatch_log_group.flink.name
}

output "producer_function_name" {
  description = "Name of the manually invoked Lambda transaction producer."
  value       = aws_lambda_function.producer.function_name
}

output "producer_log_group_name" {
  description = "CloudWatch log group for the Lambda transaction producer."
  value       = aws_cloudwatch_log_group.producer.name
}
