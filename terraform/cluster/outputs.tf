output "ecr_image" {
  description = "Image ref to build/push (scripts/push-image.sh) and that nodes run."
  value       = "${local.ecr_repo_url}:${var.image_tag}"
}

output "cold_bucket" {
  description = "S3 cold-tier bucket the cache nodes spill to / read through."
  value       = aws_s3_bucket.cold.id
}

output "etcd_public_ips" {
  description = "etcd public IPs — SSH here, or reach :2379 from your operator IP for etcdctl."
  value       = aws_instance.etcd[*].public_ip
}

output "etcd_client_endpoints_private" {
  description = "Private etcd endpoints to pass loadgen/cache as -etcd (use these from INSIDE the VPC)."
  value       = local.etcd_client_endpoints
}

output "cache_public_ips" {
  description = "Cache node public IPs. SSH here to run loadgen from inside the VPC."
  value       = aws_instance.cache[*].public_ip
}

output "prometheus_targets" {
  description = "Add these to deploy/observability/prometheus.yml (metrics open to your IP on :9090)."
  value       = [for ip in aws_instance.cache[*].public_ip : "${ip}:9090"]
}

output "loadgen_hint" {
  description = "How to drive the cluster (loadgen must run inside the VPC — cache nodes advertise private IPs)."
  value       = "ssh ec2-user@<cache_public_ip>  then:  ./loadgen -etcd ${local.etcd_client_endpoints} -verify -duration 60s -payload-bytes 262144"
}

# --- GPU benchmark node (only populated when gpu_count > 0; ADR 0032) ---
output "gpu_public_ips" {
  description = "GPU benchmark node public IPs — SSH here to install vLLM + the connector and run the distributed TTFT benchmark."
  value       = aws_instance.gpu[*].public_ip
}

output "cache_private_ips" {
  description = "Cache node PRIVATE IPs. Point the connector's --cache-addr at one of these (e.g. <ip>:50051) from the GPU node, which shares the VPC."
  value       = aws_instance.cache[*].private_ip
}
