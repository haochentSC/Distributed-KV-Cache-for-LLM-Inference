# Latest Amazon Linux 2023 x86_64 AMI via the public SSM parameter (more robust than an AMI
# name filter — AWS keeps this pointer current).
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# A concrete AZ to pin the single subnet into (everything in one AZ for low-latency gRPC).
data "aws_availability_zones" "available" {
  state = "available"
}

# Account id, used to scope the ECR/cold-tier ARNs and to make bucket names unique.
data "aws_caller_identity" "current" {}
