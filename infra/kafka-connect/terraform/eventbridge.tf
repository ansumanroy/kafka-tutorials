# Optional: EventBridge rule for scheduled backups
# Uncomment and configure if you want automated scheduled backups via EventBridge

# Lambda function for backup execution
# resource "aws_lambda_function" "backup_scheduler" {
#   filename         = "backup-lambda.zip"
#   function_name    = "kafka-backup-scheduler"
#   role            = aws_iam_role.lambda_backup.arn
#   handler         = "index.handler"
#   runtime         = "python3.9"
#   timeout         = 900  # 15 minutes
#   
#   environment {
#     variables = {
#       CONNECT_REST_URL = "http://${aws_instance.kafka_connect.private_ip}:${var.kafka_connect_rest_port}"
#       S3_BUCKET        = var.backup_s3_bucket
#     }
#   }
# }
# 
# # IAM role for Lambda
# resource "aws_iam_role" "lambda_backup" {
#   name = "kafka-backup-lambda-role"
#   
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [{
#       Effect = "Allow"
#       Principal = {
#         Service = "lambda.amazonaws.com"
#       }
#       Action = "sts:AssumeRole"
#     }]
#   })
# }
# 
# resource "aws_iam_role_policy_attachment" "lambda_basic" {
#   role       = aws_iam_role.lambda_backup.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
# }
# 
# # EventBridge rule for scheduled backups
# resource "aws_cloudwatch_event_rule" "backup_schedule" {
#   name                = "kafka-backup-schedule"
#   description         = "Scheduled trigger for Kafka backups"
#   schedule_expression = "cron(0 2 * * ? *)"  # Daily at 2 AM UTC
# }
# 
# resource "aws_cloudwatch_event_target" "backup_lambda" {
#   rule      = aws_cloudwatch_event_rule.backup_schedule.name
#   target_id = "TriggerBackupLambda"
#   arn       = aws_lambda_function.backup_scheduler.arn
# }
# 
# resource "aws_lambda_permission" "allow_eventbridge" {
#   statement_id  = "AllowExecutionFromEventBridge"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.backup_scheduler.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.backup_schedule.arn
# }

