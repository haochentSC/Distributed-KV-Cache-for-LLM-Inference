# IAM roles + instance profiles so nodes authenticate via the instance metadata service — NO
# static credentials anywhere (ADR 0004). Each role is scoped to exactly what that node needs.

data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

# --- cache node role -----------------------------------------------------------------------
resource "aws_iam_role" "cache" {
  name               = "kvcache-cache"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

# Pull the cache image from ECR (read-only managed policy is exactly this).
resource "aws_iam_role_policy_attachment" "cache_ecr" {
  role       = aws_iam_role.cache.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# Cold-tier read/write, scoped to the cold bucket ONLY (least privilege), + CloudWatch logs.
data "aws_iam_policy_document" "cache_inline" {
  statement {
    sid       = "ColdTierRW"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.cold.arn}/*"]
  }
  statement {
    sid       = "ColdTierList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.cold.arn]
  }
  statement {
    sid       = "CloudWatchLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"]
    resources = ["${aws_cloudwatch_log_group.cache.arn}:*"]
  }
}

resource "aws_iam_role_policy" "cache_inline" {
  name   = "kvcache-cache-inline"
  role   = aws_iam_role.cache.id
  policy = data.aws_iam_policy_document.cache_inline.json
}

resource "aws_iam_instance_profile" "cache" {
  name = "kvcache-cache"
  role = aws_iam_role.cache.name
}

# --- etcd node role ------------------------------------------------------------------------
resource "aws_iam_role" "etcd" {
  name               = "kvcache-etcd"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "etcd_inline" {
  statement {
    sid       = "CloudWatchLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup"]
    resources = ["${aws_cloudwatch_log_group.etcd.arn}:*"]
  }
}

resource "aws_iam_role_policy" "etcd_inline" {
  name   = "kvcache-etcd-inline"
  role   = aws_iam_role.etcd.id
  policy = data.aws_iam_policy_document.etcd_inline.json
}

resource "aws_iam_instance_profile" "etcd" {
  name = "kvcache-etcd"
  role = aws_iam_role.etcd.name
}
