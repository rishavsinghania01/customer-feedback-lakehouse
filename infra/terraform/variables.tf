variable "aws_region" {
  description = "AWS region for the lakehouse."
  type        = string
  default     = "ap-south-1"
}

variable "project_name" {
  description = "Prefix used for resource names."
  type        = string
  default     = "customer-feedback-lakehouse"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod"
  }
}

variable "kinesis_shard_count" {
  description = "Provisioned shards for the feedback event stream."
  type        = number
  default     = 1
}
