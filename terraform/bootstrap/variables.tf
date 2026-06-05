variable "region" {
  description = "AWS region for the remote-state backend (keep it the same as the cluster)."
  type        = string
  default     = "us-east-1"
}

variable "state_bucket" {
  description = "Globally-unique S3 bucket name for Terraform remote state."
  type        = string
  # No default: bucket names are global, so the operator must pick a unique one
  # (e.g. kvcache-tfstate-<account-id>).
}

variable "lock_table" {
  description = "DynamoDB table name for Terraform state locking."
  type        = string
  default     = "kvcache-tflock"
}
