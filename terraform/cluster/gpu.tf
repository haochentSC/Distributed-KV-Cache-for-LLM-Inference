# GPU benchmark node (Phase 4.5 distributed TTFT; ADR 0032). OFF by default (gpu_count = 0) so a
# normal `apply` never creates it — this is the only hourly-billed GPU resource in the project.
# Lifecycle: `terraform apply -var gpu_count=1` for the paid window, run the benchmark from here
# (vLLM with tensor_parallel_size = N points the connector at a cache node's PRIVATE IP over the
# VPC), then `terraform apply -var gpu_count=0` / `terraform destroy`. See docs/benchmarks/
# aws-batch-runbook.md.
#
# It needs no IAM (no ECR/S3 — vLLM + our connector are installed via pip in the runbook) and no
# new cache-side rule: aws_security_group.cache already allows gRPC 50051 from the whole vpc_cidr,
# and this node sits in the same public subnet.

# Deep Learning AMI (NVIDIA driver + PyTorch preinstalled). Resolved from SSM only when a GPU node
# is actually requested and no explicit gpu_ami_id override is given — so gpu_count = 0 does no
# lookup, and a drifted SSM path can be bypassed by setting gpu_ami_id.
data "aws_ssm_parameter" "dlami" {
  count = var.gpu_count > 0 && var.gpu_ami_id == "" ? 1 : 0
  name  = var.gpu_ami_ssm_param
}

resource "aws_security_group" "gpu" {
  count       = var.gpu_count > 0 ? 1 : 0
  name        = "kvcache-gpu"
  description = "GPU benchmark node: SSH from operator; all egress (reaches cache gRPC inside the VPC)"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "kvcache-gpu" }
}

resource "aws_vpc_security_group_ingress_rule" "gpu_ssh" {
  count             = var.gpu_count > 0 ? 1 : 0
  security_group_id = aws_security_group.gpu[0].id
  description       = "SSH from operator IP"
  cidr_ipv4         = var.my_ip_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "gpu_egress" {
  count             = var.gpu_count > 0 ? 1 : 0
  security_group_id = aws_security_group.gpu[0].id
  description       = "all egress (cache gRPC within VPC, Hugging Face model pulls, pip)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "gpu" {
  count                  = var.gpu_count
  ami                    = var.gpu_ami_id != "" ? var.gpu_ami_id : one(data.aws_ssm_parameter.dlami[*].value)
  instance_type          = var.gpu_instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.gpu[0].id]
  key_name               = var.key_name != "" ? var.key_name : null

  # Spot keeps the cost of the one paid window down. A reclaim mid-benchmark just means re-running it;
  # the benchmark is idempotent (the cache is populated on the cold pass, served on the warm pass).
  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type = "one-time"
    }
  }

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required" # IMDSv2
  }

  # Big enough for CUDA/PyTorch + the Hugging Face weights cache (a 30B model is ~60 GB on disk).
  root_block_device {
    volume_size = var.gpu_root_gb
    volume_type = "gp3"
  }

  tags = { Name = "kvcache-gpu-${count.index}" }
}
