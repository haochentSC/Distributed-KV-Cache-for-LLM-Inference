# etcd quorum: on-demand instances (never Spot, ADR 0009) with FIXED private IPs, so the static
# initial-cluster string (locals.etcd_initial_cluster) is known before the instances exist — no
# discovery service needed. user-data installs etcd v3.5.17 (matches the Go client dep) and writes
# a systemd unit bootstrapped with the full member list.
resource "aws_instance" "etcd" {
  count                  = var.etcd_count
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.etcd_instance_type
  subnet_id              = aws_subnet.public.id
  private_ip             = local.etcd_ips[count.index]
  vpc_security_group_ids = [aws_security_group.etcd.id]
  iam_instance_profile   = aws_iam_instance_profile.etcd.name
  key_name               = var.key_name != "" ? var.key_name : null

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required" # IMDSv2
  }

  user_data = templatefile("${path.module}/userdata/etcd.sh.tftpl", {
    node_index      = count.index
    node_ip         = local.etcd_ips[count.index]
    initial_cluster = local.etcd_initial_cluster
  })

  tags = { Name = "kvcache-etcd-${count.index}" }
}
