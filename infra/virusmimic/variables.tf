variable "region" {
  description = "AWS region for the placeholder scan Lambda"
  type        = string
}

variable "env" {
  description = "Environment name (e.g. dev, staging, prod) used to prefix resource names and tags"
  type        = string
  default     = "dev"
}

variable "staging_bucket_name" {
  description = "Source bucket where new objects arrive to be scanned"
  type        = string
  default     = "sf-bucket-staging"
}

variable "scanned_bucket_name" {
  description = "Destination bucket for successfully scanned objects"
  type        = string
  default     = "sf-bucket-scanned"
}

variable "infected_bucket_name" {
  description = "Destination bucket for infected/failed objects"
  type        = string
  default     = "sf-bucket-infected"
}

variable "lambda_timeout_seconds" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_mb" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 512
}

variable "scan_sleep_seconds" {
  description = "How long the placeholder Lambda should sleep to mimic scan duration"
  type        = number
  default     = 120
}

variable "delete_from_staging" {
  description = "Whether to delete the object from the staging bucket after moving"
  type        = bool
  default     = true
}

variable "clean_probability" {
  description = "Probability (0-1) that a file is considered clean by the placeholder scan"
  type        = number
  default     = 0.9
}

variable "salesforce_endpoint_url" {
  description = "Salesforce REST API endpoint to call after scan completes"
  type        = string
  default     = ""
}

variable "salesforce_jwt_secret_arn" {
  description = "ARN of Secrets Manager secret containing Salesforce JWT token (SecretString)"
  type        = string
  default     = ""
}

variable "salesforce_timeout_seconds" {
  description = "Timeout in seconds for Salesforce HTTP calls"
  type        = number
  default     = 5
}


