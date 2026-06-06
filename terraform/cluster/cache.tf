# Cache nodes on Spot (ADR 0006). Individual instances (not an ASG) so the chaos harness can
# cleanly terminate one and watch failover — a managed group would just replace it and contaminate
# the recovery measurement (ADR 0005). user-data runs the ECR image under a systemd unit; SIGTERM
# on stop drives the graceful drain (ADR 0023).
resource "aws_instance" "cache" {
  count                  = var.cache_count
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.cache_instance_type
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.cache.id]
  iam_instance_profile   = aws_iam_instance_profile.cache.name
  key_name               = var.key_name != "" ? var.key_name : null

  # Spot: market_type=spot with the default (on-demand) price cap.
  instance_market_options {
    market_type = "spot"
    spot_options {
      spot_instance_type = "one-time"
    }
  }

  # Burstable (t3.*) credit mode. The cache is CPU-bound under load test + chaos, and a depleted
  # t3 credit balance throttles the node to its ~0.2 vCPU baseline — which starves the etcd lease
  # keepalive and drops the node from the ring. "unlimited" lets it burst to full vCPU (a small
  # surplus charge only while bursting). Ignored by AWS for non-burstable types if you resize.
  credit_specification {
    cpu_credits = "unlimited"
  }

  # IMDSv2 with hop limit 2 so the in-CONTAINER Spot poller can still reach the metadata service
  # (the extra hop is the container network namespace).
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  # Recreate the node when the bootstrap template changes (so user-data fixes actually take effect;
  # the default would update the attribute in place without re-running it).
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/userdata/cache.sh.tftpl", {
    region         = var.region
    ecr_registry   = "${local.account_id}.dkr.ecr.${var.region}.amazonaws.com"
    ecr_image      = "${local.ecr_repo_url}:${var.image_tag}"
    etcd_endpoints = local.etcd_client_endpoints
    cold_bucket    = local.cold_bucket
    max_bytes      = var.cache_max_bytes
    rf             = var.rf
    lease_ttl      = var.lease_ttl
    log_group      = aws_cloudwatch_log_group.cache.name
  })

  # Bring etcd up first so a cache node can register on boot (it also retries via systemd if not).
  depends_on = [aws_instance.etcd]

  tags = { Name = "kvcache-cache-${count.index}" }
}
