output "state_bucket" {
  description = "S3 bucket holding Terraform remote state — put this in cluster/versions.tf backend."
  value       = aws_s3_bucket.state.id
}

output "lock_table" {
  description = "DynamoDB lock table — put this in cluster/versions.tf backend."
  value       = aws_dynamodb_table.lock.name
}

output "region" {
  description = "Region the backend lives in."
  value       = var.region
}
