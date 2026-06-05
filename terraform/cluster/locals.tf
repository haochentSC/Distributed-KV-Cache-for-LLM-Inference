locals {
  az          = data.aws_availability_zones.available.names[0]
  account_id  = data.aws_caller_identity.current.account_id
  cold_bucket = "kvcache-cold-${local.account_id}"

  # Fixed private IPs for etcd so user-data can template a STATIC initial-cluster string before the
  # instances exist (no discovery service needed). .10, .11, .12 in the subnet.
  etcd_ips = [for i in range(var.etcd_count) : cidrhost(var.subnet_cidr, 10 + i)]

  # etcd client endpoints the cache nodes dial (comma-separated host:2379).
  etcd_client_endpoints = join(",", [for ip in local.etcd_ips : "${ip}:2379"])

  # The static initial-cluster string every etcd node is bootstrapped with:
  #   etcd-0=http://10.0.1.10:2380,etcd-1=http://10.0.1.11:2380,...
  etcd_initial_cluster = join(",", [for i, ip in local.etcd_ips : "etcd-${i}=http://${ip}:2380"])

  ecr_repo_url = aws_ecr_repository.cache.repository_url
}
