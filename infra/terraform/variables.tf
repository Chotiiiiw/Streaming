variable "aws_region" {
  description = "AWS region used for project resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Local AWS CLI profile used by Terraform."
  type        = string
  default     = "streaming-learning"
}

variable "project_name" {
  description = "Name used to identify and tag project resources."
  type        = string
  default     = "streaming-fraud-detection"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "learning"
}

variable "vpc_cidr" {
  description = "CIDR range for the project VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR ranges for private subnets in separate availability zones."
  type        = list(string)

  default = [
    "10.20.1.0/24",
    "10.20.2.0/24"
  ]

  validation {
    condition     = length(var.private_subnet_cidrs) == 2
    error_message = "Exactly two private subnet CIDRs must be provided."
  }
}

variable "flink_jar_path" {
  description = "Path to the locally built shaded Flink application JAR, relative to this Terraform directory unless absolute."
  type        = string
  default     = "../../flink-app/target/transaction-routing-job.jar"
}

variable "flink_runtime_environment" {
  description = "Managed Service for Apache Flink runtime matching the application build."
  type        = string
  default     = "FLINK-2_2"
}

variable "lambda_producer_zip_path" {
  description = "Path to the packaged Lambda producer ZIP, relative to this Terraform directory unless absolute."
  type        = string
  default     = "../../producer/build/lambda-producer.zip"
}

variable "flink_start_application" {
  description = "Whether Terraform should start the Managed Flink application. Keep false while reviewing to avoid Flink runtime charges."
  type        = bool
  default     = false
}

variable "kafka_topics" {
  description = "Kafka topics used by the transaction-routing application."

  type = object({
    transactions_raw   = string
    clean_transactions = string
    fraud_alerts       = string
    transactions_dlq   = string
  })

  default = {
    transactions_raw   = "transactions_raw"
    clean_transactions = "clean_transactions"
    fraud_alerts       = "fraud_alerts"
    transactions_dlq   = "transactions_dlq"
  }
}

variable "kafka_group_id" {
  description = "Kafka consumer group used by the transaction-routing application."
  type        = string
  default     = "transaction-router-v1"
}
