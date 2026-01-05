terraform {
  required_version = ">= 1.3"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = ">= 2.4"
    }
  }
}

provider "aws" {
  region = var.region
}

data "aws_caller_identity" "current" {}

locals {
  # Prefix to segment resources per environment, e.g. virusmimic-dev
  env_prefix  = "virusmimic-${var.env}"
  lambda_name = "${local.env_prefix}-scan-mimic-lambda"
}

resource "aws_cloudwatch_log_group" "scan_mimic" {
  name              = "/aws/lambda/${locals.lambda_name}"
  retention_in_days = 14

  tags = {
    Name = "/aws/lambda/${locals.lambda_name}"
    Env  = var.env
  }
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scan_mimic_role" {
  name               = "${local.env_prefix}-scan-mimic-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Name = "${local.env_prefix}-scan-mimic-role"
    Env  = var.env
  }
}

data "aws_iam_policy_document" "scan_mimic_policy" {
  statement {
    sid    = "AllowS3ObjectOps"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectTagging",
      "s3:PutObjectTagging",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "arn:aws:s3:::${var.staging_bucket_name}/*",
      "arn:aws:s3:::${var.scanned_bucket_name}/*",
      "arn:aws:s3:::${var.infected_bucket_name}/*",
    ]
  }

  statement {
    sid    = "AllowCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${locals.lambda_name}:*",
      "arn:aws:logs:${var.region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${locals.lambda_name}",
    ]
  }

  # Allow reading Salesforce JWT from Secrets Manager (if configured)
  statement {
    sid    = "AllowSecretsManagerJWT"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
    ]
    resources = [
      var.salesforce_jwt_secret_arn,
    ]
  }
}

resource "aws_iam_policy" "scan_mimic_policy" {
  name   = "${local.env_prefix}-scan-mimic-policy"
  policy = data.aws_iam_policy_document.scan_mimic_policy.json
}

resource "aws_iam_role_policy_attachment" "scan_mimic_attach" {
  role       = aws_iam_role.scan_mimic_role.name
  policy_arn = aws_iam_policy.scan_mimic_policy.arn
}

resource "aws_lambda_function" "scan_mimic" {
  function_name = locals.lambda_name
  role          = aws_iam_role.scan_mimic_role.arn

  runtime = "python3.11"
  handler = "scan_mimic.lambda_handler"

  filename         = data.archive_file.scan_mimic_zip.output_path
  source_code_hash = data.archive_file.scan_mimic_zip.output_base64sha256

  timeout      = var.lambda_timeout_seconds
  memory_size  = var.lambda_memory_mb
  publish      = true

  environment {
    variables = {
      STAGING_BUCKET      = var.staging_bucket_name
      SCANNED_BUCKET      = var.scanned_bucket_name
      INFECTED_BUCKET     = var.infected_bucket_name
      SCAN_SLEEP_SECONDS  = tostring(var.scan_sleep_seconds)
      DELETE_FROM_STAGING = tostring(var.delete_from_staging)
      CLEAN_PROBABILITY   = tostring(var.clean_probability)
      SALESFORCE_ENDPOINT_URL   = var.salesforce_endpoint_url
      SALESFORCE_JWT_SECRET_ARN = var.salesforce_jwt_secret_arn
      SALESFORCE_TIMEOUT_SECONDS = tostring(var.salesforce_timeout_seconds)
    }
  }

  depends_on = [aws_cloudwatch_log_group.scan_mimic]

  tags = {
    Name = locals.lambda_name
    Env  = var.env
  }
}

data "archive_file" "scan_mimic_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda/scan_mimic.py"
  output_path = "${path.module}/lambda/scan_mimic.zip"
}

resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowExecutionFromS3-${var.env}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scan_mimic.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.staging_bucket_name}"
}

resource "aws_s3_bucket_notification" "staging_notify" {
  bucket = var.staging_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.scan_mimic.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}

