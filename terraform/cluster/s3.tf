# Cold-tier bucket (ADR 0027): cache nodes spill evicted blocks here and read them back through.
resource "aws_s3_bucket" "cold" {
  bucket        = local.cold_bucket
  force_destroy = true # cold data is disposable (a miss just recomputes), so allow clean teardown
  tags          = { Name = "kvcache-cold" }
}

resource "aws_s3_bucket_public_access_block" "cold" {
  bucket                  = aws_s3_bucket.cold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Cold objects are disposable: expire them so the bucket doesn't grow forever (ADR 0027 follow-up).
resource "aws_s3_bucket_lifecycle_configuration" "cold" {
  bucket = aws_s3_bucket.cold.id
  rule {
    id     = "expire-cold-blocks"
    status = "Enabled"
    filter {
      prefix = "blocks/"
    }
    expiration {
      days = 7
    }
  }
}
