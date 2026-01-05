variable "region" {
  description = "AWS region"
  type        = string
}

variable "name_prefix" {
  description = "Prefix for created resources"
  type        = string
  default     = "virusscan"
}

variable "vpc_id" {
  description = "VPC ID for the ASG"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for the ASG"
  type        = list(string)
}

variable "scan_bucket_arns" {
  description = "List of S3 bucket ARNs whose objects will be scanned"
  type        = list(string)
}

variable "instance_type" {
  description = "Instance type for scanner nodes"
  type        = string
  default     = "t3.medium"
}

variable "asg_min_size" {
  description = "Min ASG size"
  type        = number
  default     = 1
}

variable "asg_max_size" {
  description = "Max ASG size"
  type        = number
  default     = 5
}

variable "asg_desired_capacity" {
  description = "Desired ASG size"
  type        = number
  default     = 1
}
