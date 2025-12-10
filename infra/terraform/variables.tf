variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
  default     = "us-east-1"
}

variable "stack_name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "kafka-dev-image-builder"
}

variable "vpc_id" {
  description = "VPC ID where the Image Builder instances will run"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for the Image Builder instances (should have internet access)"
  type        = string
}

variable "security_group_id" {
  description = "Security Group ID for the Image Builder instances"
  type        = string
}

variable "instance_type" {
  description = "Instance type for building the AMI"
  type        = string
  default     = "t3.medium"

  validation {
    condition = contains([
      "t3.medium",
      "t3.large",
      "t3.xlarge",
      "m5.large",
      "m5.xlarge"
    ], var.instance_type)
    error_message = "Instance type must be one of: t3.medium, t3.large, t3.xlarge, m5.large, m5.xlarge"
  }
}

variable "target_regions" {
  description = "List of regions to distribute the AMI to (excluding the current region)"
  type        = list(string)
  default     = []
}

variable "notification_email" {
  description = "Email address for build notifications (optional)"
  type        = string
  default     = ""
}

variable "base_image_id" {
  description = "Amazon Linux 2 base AMI ID (update for your region if needed)"
  type        = string
  default     = "ami-0c02fb55b7c22fdef"  # Amazon Linux 2 in us-east-1
}

variable "component_version" {
  description = "Version number for the Image Builder component"
  type        = string
  default     = "1.0.0"
}

variable "ami_user_ids" {
  description = "AWS account IDs to share the AMI with"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}
