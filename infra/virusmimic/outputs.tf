output "scan_mimic_lambda_arn" {
  description = "ARN of the scan-mimic Lambda function"
  value       = aws_lambda_function.scan_mimic.arn
}

output "scan_mimic_log_group" {
  description = "CloudWatch Logs log group for the scan-mimic Lambda"
  value       = aws_cloudwatch_log_group.scan_mimic.name
}

