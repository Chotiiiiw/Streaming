locals {
  lambda_producer_zip_path = abspath(var.lambda_producer_zip_path)
}

resource "aws_security_group" "producer" {
  name_prefix = "${var.project_name}-${var.environment}-producer-"
  description = "Network access for the Lambda transaction producer"
  vpc_id      = aws_vpc.main.id

  revoke_rules_on_delete = true

  tags = {
    Name = "${var.project_name}-${var.environment}-producer"
  }
}

resource "aws_vpc_security_group_egress_rule" "producer_to_msk" {
  security_group_id = aws_security_group.producer.id

  referenced_security_group_id = aws_security_group.msk.id

  from_port   = 9098
  to_port     = 9098
  ip_protocol = "tcp"

  description = "Allow the Lambda producer to reach the MSK IAM endpoint"
}

resource "aws_vpc_security_group_ingress_rule" "msk_from_producer" {
  security_group_id = aws_security_group.msk.id

  referenced_security_group_id = aws_security_group.producer.id

  from_port   = 9098
  to_port     = 9098
  ip_protocol = "tcp"

  description = "Allow the Lambda producer to connect using MSK IAM"
}

data "aws_iam_policy_document" "producer_assume_role" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole"
    ]

    principals {
      type = "Service"

      identifiers = [
        "lambda.amazonaws.com"
      ]
    }
  }
}

resource "aws_iam_role" "producer" {
  name               = "${var.project_name}-${var.environment}-producer"
  assume_role_policy = data.aws_iam_policy_document.producer_assume_role.json

  tags = {
    Name = "${var.project_name}-${var.environment}-producer"
  }
}

data "aws_iam_policy_document" "producer" {
  statement {
    sid    = "WriteLogs"
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = [
      "${aws_cloudwatch_log_group.producer.arn}:*"
    ]
  }

  statement {
    sid    = "ManageVpcNetworkInterfaces"
    effect = "Allow"

    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DescribeSubnets",
      "ec2:DeleteNetworkInterface",
      "ec2:AssignPrivateIpAddresses",
      "ec2:UnassignPrivateIpAddresses"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ConnectToMsk"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster"
    ]

    resources = [
      aws_msk_serverless_cluster.main.arn
    ]
  }

  statement {
    sid    = "CreateAndWriteTopics"
    effect = "Allow"

    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ]

    resources = [
      local.msk_topic_arn
    ]
  }
}

resource "aws_iam_role_policy" "producer" {
  name   = "${var.project_name}-${var.environment}-producer-access"
  role   = aws_iam_role.producer.id
  policy = data.aws_iam_policy_document.producer.json
}

resource "aws_cloudwatch_log_group" "producer" {
  name              = "/aws/lambda/${var.project_name}-${var.environment}-producer"
  retention_in_days = 7

  tags = {
    Name = "${var.project_name}-${var.environment}-producer"
  }
}

resource "aws_lambda_function" "producer" {
  function_name = "${var.project_name}-${var.environment}-producer"
  description   = "Initializes MSK topics and publishes simulated transactions."

  filename = local.lambda_producer_zip_path
  source_code_hash = fileexists(local.lambda_producer_zip_path) ? (
    filebase64sha256(local.lambda_producer_zip_path)
  ) : null

  role    = aws_iam_role.producer.arn
  handler = "lambda_handler.lambda_handler"
  runtime = "python3.13"

  memory_size = 512
  timeout     = 120

  vpc_config {
    subnet_ids = aws_subnet.private[*].id

    security_group_ids = [
      aws_security_group.producer.id
    ]
  }

  environment {
    variables = {
      KAFKA_BOOTSTRAP_SERVERS  = aws_msk_serverless_cluster.main.bootstrap_brokers_sasl_iam
      TRANSACTIONS_RAW_TOPIC   = var.kafka_topics.transactions_raw
      CLEAN_TRANSACTIONS_TOPIC = var.kafka_topics.clean_transactions
      FRAUD_ALERTS_TOPIC       = var.kafka_topics.fraud_alerts
      TRANSACTIONS_DLQ_TOPIC   = var.kafka_topics.transactions_dlq
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.producer,
    aws_iam_role_policy.producer
  ]

  lifecycle {
    precondition {
      condition     = fileexists(local.lambda_producer_zip_path)
      error_message = "Build the Lambda package first with: ../../producer/build_lambda_package.sh"
    }
  }

  tags = {
    Name = "${var.project_name}-${var.environment}-producer"
  }
}
