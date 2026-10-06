resource "aws_kinesisanalyticsv2_application" "flink" {
  name        = "${var.project_name}-${var.environment}-flink"
  description = "Routes transactions and calculates fraud risk scores from MSK events."

  runtime_environment    = var.flink_runtime_environment
  service_execution_role = aws_iam_role.flink.arn
  application_mode       = "STREAMING"

  # Creating the application in READY state avoids Flink runtime charges until
  # the learning environment is deliberately started.
  start_application = var.flink_start_application
  force_stop        = true

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"

      code_content {
        s3_content_location {
          bucket_arn     = aws_s3_bucket.artifacts.arn
          file_key       = aws_s3_object.flink_application.key
          object_version = aws_s3_object.flink_application.version_id
        }
      }
    }

    environment_properties {
      property_group {
        property_group_id = "KafkaConfigProperties"

        property_map = {
          KAFKA_BOOTSTRAP_SERVERS     = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
          TRANSACTIONS_RAW_TOPIC      = var.kafka_topics.transactions_raw
          CLEAN_TRANSACTIONS_TOPIC    = var.kafka_topics.clean_transactions
          FRAUD_ALERTS_TOPIC          = var.kafka_topics.fraud_alerts
          TRANSACTIONS_DLQ_TOPIC      = var.kafka_topics.transactions_dlq
          KAFKA_GROUP_ID              = var.kafka_group_id
          KAFKA_STARTUP_MODE          = "latest-offset"
          S3_OUTPUT_PATH              = "s3a://${aws_s3_bucket.data.id}/curated/transactions"
          KAFKA_SECURITY_PROTOCOL     = "SASL_SSL"
          KAFKA_SASL_MECHANISM        = "AWS_MSK_IAM"
          KAFKA_SASL_JAAS_CONFIG      = "software.amazon.msk.auth.iam.IAMLoginModule required;"
          KAFKA_SASL_CALLBACK_HANDLER = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = false
        parallelism          = 1
        parallelism_per_kpu  = 1
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = true
    }

    vpc_configuration {
      subnet_ids = aws_subnet.private[*].id

      security_group_ids = [
        aws_security_group.flink.id
      ]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  depends_on = [
    aws_iam_role_policy.flink
  ]

  tags = {
    Name = "${var.project_name}-${var.environment}-flink"
  }
}
