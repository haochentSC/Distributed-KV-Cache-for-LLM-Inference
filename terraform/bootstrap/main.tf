# Remote-state backend: an S3 bucket (versioned, encrypted, private) for state and a DynamoDB
# table for the state lock. Apply this ONCE, then the cluster module points its backend here.

resource "aws_s3_bucket" "state" {
  bucket = var.state_bucket

  # Guard rail: refuse `terraform destroy` of the bucket that holds everyone's state. Flip this to
  # false deliberately if you ever really mean to tear the backend down.
  lifecycle {
    prevent_destroy = true
  }
}

# Versioning keeps a history of state files so a bad apply can be rolled back.
resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encrypt state at rest (it can contain sensitive values).
resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# State must never be public.
resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# State lock: prevents two `apply`s from racing on the same state. PAY_PER_REQUEST so it costs
# effectively nothing at this scale.
resource "aws_dynamodb_table" "lock" {
  name         = var.lock_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}
