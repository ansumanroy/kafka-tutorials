output "kafka_connect_instance_id" {
  description = "EC2 instance ID for Kafka Connect"
  value       = aws_instance.kafka_connect.id
}

output "kafka_connect_private_ip" {
  description = "Private IP address of Kafka Connect instance"
  value       = aws_instance.kafka_connect.private_ip
}

output "kafka_connect_security_group_id" {
  description = "Security group ID for Kafka Connect"
  value       = aws_security_group.kafka_connect.id
}

output "kafka_connect_rest_url" {
  description = "Kafka Connect REST API URL"
  value       = "http://${aws_instance.kafka_connect.private_ip}:${var.kafka_connect_rest_port}"
}

output "msk_bootstrap_servers" {
  description = "MSK cluster bootstrap servers"
  value       = data.aws_msk_cluster.main.bootstrap_brokers
}

output "iam_role_arn" {
  description = "IAM role ARN for Kafka Connect"
  value       = aws_iam_role.kafka_connect.arn
}

