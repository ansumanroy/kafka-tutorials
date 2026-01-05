variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "msk_cluster_name" {
  description = "Name of the MSK cluster to connect to"
  type        = string
}

variable "msk_cluster_arn" {
  description = "ARN of the MSK cluster"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID where Kafka Connect will be deployed"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for Kafka Connect EC2 instance"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for Kafka Connect"
  type        = list(string)
  default     = []
}

variable "instance_type" {
  description = "EC2 instance type for Kafka Connect"
  type        = string
  default     = "t3.medium"
}

variable "backup_s3_bucket" {
  description = "S3 bucket name for Kafka backups (must already exist)"
  type        = string
}

variable "kafka_connect_rest_port" {
  description = "Port for Kafka Connect REST API"
  type        = number
  default     = 8083
}

variable "kafka_connect_key_name" {
  description = "EC2 key pair name for SSH access (optional)"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

