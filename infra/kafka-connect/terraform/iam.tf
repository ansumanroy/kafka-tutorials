terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# IAM Role for Kafka Connect EC2 instance
resource "aws_iam_role" "kafka_connect" {
  name = "kafka-connect-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name = "kafka-connect-role"
    }
  )
}

# IAM Policy for S3 access
resource "aws_iam_role_policy" "s3_backup_access" {
  name = "KafkaConnectS3BackupAccess"
  role = aws_iam_role.kafka_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetObjectTagging",
          "s3:PutObjectTagging"
        ]
        Resource = [
          "arn:aws:s3:::${var.backup_s3_bucket}",
          "arn:aws:s3:::${var.backup_s3_bucket}/*"
        ]
      }
    ]
  })
}

# IAM Policy for MSK access (if using IAM authentication)
resource "aws_iam_role_policy" "msk_access" {
  name = "KafkaConnectMSKAccess"
  role = aws_iam_role.kafka_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeCluster",
          "kafka-cluster:DescribeClusterDynamicConfiguration",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData"
        ]
        Resource = [
          var.msk_cluster_arn
        ]
      }
    ]
  })
}

# IAM Policy for CloudWatch Logs
resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = "KafkaConnectCloudWatchLogs"
  role = aws_iam_role.kafka_connect.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/kafka-connect/*"
      }
    ]
  })
}

# IAM Instance Profile
resource "aws_iam_instance_profile" "kafka_connect" {
  name = "kafka-connect-instance-profile"
  role = aws_iam_role.kafka_connect.name

  tags = merge(
    var.tags,
    {
      Name = "kafka-connect-instance-profile"
    }
  )
}

