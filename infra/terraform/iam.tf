data "aws_partition" "current" {}

locals {
  msk_topic_arn = format(
    "%s/*",
    replace(
      aws_msk_serverless_cluster.main.arn,
      ":cluster/",
      ":topic/"
    )
  )

  msk_group_arn = format(
    "%s/*",
    replace(
      aws_msk_serverless_cluster.main.arn,
      ":cluster/",
      ":group/"
    )
  )
}

data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole"
    ]

    principals {
      type = "Service"

      identifiers = [
        "kinesisanalytics.amazonaws.com"
      ]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"

      values = [
        data.aws_caller_identity.current.account_id
      ]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"

      values = [
        "arn:${data.aws_partition.current.partition}:kinesisanalytics:${var.aws_region}:${data.aws_caller_identity.current.account_id}:application/*"
      ]
    }
  }
}

resource "aws_iam_role" "flink" {
  name = "${var.project_name}-${var.environment}-flink-execution"

  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json

  tags = {
    Name = "${var.project_name}-${var.environment}-flink-execution"
  }
}

data "aws_iam_policy_document" "flink" {
  statement {
    sid    = "ReadApplicationArtifact"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion"
    ]

    resources = [
      "${aws_s3_bucket.artifacts.arn}/*"
    ]
  }

  statement {
    sid    = "ListArtifactBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      aws_s3_bucket.artifacts.arn
    ]
  }

  statement {
    sid    = "ListTransactionDataBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads"
    ]

    resources = [
      aws_s3_bucket.data.arn
    ]
  }

  statement {
    sid    = "WriteTransactionData"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
      "s3:PutObject"
    ]

    resources = [
      "${aws_s3_bucket.data.arn}/*"
    ]
  }

  statement {
    sid    = "DescribeLogs"
    effect = "Allow"

    actions = [
      "logs:DescribeLogGroups"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "WriteApplicationLogs"
    effect = "Allow"

    actions = [
      "logs:DescribeLogStreams",
      "logs:PutLogEvents"
    ]

    resources = [
      aws_cloudwatch_log_group.flink.arn,
      aws_cloudwatch_log_stream.flink.arn
    ]
  }

  statement {
    sid    = "DescribeVpc"
    effect = "Allow"

    actions = [
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeDhcpOptions"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ManageNetworkInterfaces"
    effect = "Allow"

    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "ConnectToMsk"
    effect = "Allow"

    actions = [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeCluster",
      "kafka-cluster:WriteDataIdempotently"
    ]

    resources = [
      aws_msk_serverless_cluster.main.arn
    ]
  }

  statement {
    sid    = "AccessKafkaTopics"
    effect = "Allow"

    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:WriteData"
    ]

    resources = [
      local.msk_topic_arn
    ]
  }

  statement {
    sid    = "AccessKafkaConsumerGroups"
    effect = "Allow"

    actions = [
      "kafka-cluster:DescribeGroup",
      "kafka-cluster:AlterGroup"
    ]

    resources = [
      local.msk_group_arn
    ]
  }
}

resource "aws_iam_role_policy" "flink" {
  name = "${var.project_name}-${var.environment}-flink-access"
  role = aws_iam_role.flink.id

  policy = data.aws_iam_policy_document.flink.json
}
