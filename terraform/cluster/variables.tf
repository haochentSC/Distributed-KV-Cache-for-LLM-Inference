variable "region" {
  description = "AWS region. Single region, single AZ for low-latency intra-cluster gRPC."
  type        = string
  default     = "us-east-1"
}

variable "my_ip_cidr" {
  description = "Your public IP as a /32 CIDR (e.g. 203.0.113.4/32). SSH and the metrics port are opened ONLY to this. Find it with: curl -s https://checkip.amazonaws.com"
  type        = string
}

variable "key_name" {
  description = "Name of an existing EC2 key pair for SSH access to the nodes. Empty disables SSH key assignment."
  type        = string
  default     = ""
}

variable "vpc_cidr" {
  description = "CIDR for the cluster VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "subnet_cidr" {
  description = "CIDR for the single public subnet. etcd nodes take .10/.11/.12 in here (see locals)."
  type        = string
  default     = "10.0.1.0/24"
}

variable "etcd_count" {
  description = "Number of etcd nodes (on-demand, never Spot; ADR 0009). 3 = real quorum; 1 = cheap fallback."
  type        = number
  default     = 3
}

variable "etcd_instance_type" {
  description = "Instance type for etcd nodes (on-demand). Small is fine — etcd here is metadata-only."
  type        = string
  default     = "t3.small"
}

variable "cache_count" {
  description = "Number of cache nodes (Spot). Individual instances (not an ASG) so chaos can cleanly terminate one."
  type        = number
  default     = 3
}

variable "cache_instance_type" {
  description = "Instance type for cache nodes (Spot). Size for RAM (Revision E): ~1 GB per 500-token prefix at 2 MB/token."
  type        = string
  default     = "t3.small"
}

variable "cache_max_bytes" {
  description = "Per-node -max-bytes (the LRU/cold-tier byte budget). ~70% of the instance RAM. t3.small has 2 GiB."
  type        = number
  default     = 1500000000 # ~1.4 GiB
}

variable "rf" {
  description = "Replication factor the cache servers run with (ADR 0021)."
  type        = number
  default     = 2
}

variable "lease_ttl" {
  description = "etcd membership lease TTL (seconds) — the failure-detection / recovery window."
  type        = number
  default     = 10
}

variable "image_tag" {
  description = "ECR image tag the cache nodes run (push it with scripts/push-image.sh before apply)."
  type        = string
  default     = "latest"
}

# --- GPU benchmark node (Phase 4.5 distributed TTFT; ADR 0032) -----------------------------------
# This is the ONLY expensive resource in the project and is OFF by default. It exists solely to run
# the distributed vLLM -> multi-node cache TTFT benchmark in one paid window, then be destroyed.
# Bring it up with `-var gpu_count=1`; tear it down with `-var gpu_count=0` (or terraform destroy).
variable "gpu_count" {
  description = "Number of GPU benchmark nodes. 0 = created by nothing (default; no GPU bill). Set to 1 only for the paid benchmark window, then back to 0."
  type        = number
  default     = 0
}

variable "gpu_instance_type" {
  description = "GPU instance for the TTFT benchmark. g5.12xlarge = 4x A10G (96 GB) — fits a ~30B bf16 model with tensor_parallel_size=4. Bills hourly: destroy after the run."
  type        = string
  default     = "g5.12xlarge"
}

variable "gpu_root_gb" {
  description = "Root EBS size (GiB) for the GPU node — must hold CUDA/PyTorch + the Hugging Face weights cache (a 30B model is ~60 GB on disk)."
  type        = number
  default     = 200
}

variable "gpu_ami_id" {
  description = "Explicit AMI id for the GPU node (an AWS Deep Learning AMI with NVIDIA driver + PyTorch). Leave empty to resolve gpu_ami_ssm_param instead. Set this if the SSM path has drifted."
  type        = string
  default     = ""
}

variable "gpu_ami_ssm_param" {
  description = "SSM public parameter resolving to a Deep Learning AMI id. Best-effort default; VERIFY in the console before the window (these paths drift across DLAMI releases), or set gpu_ami_id directly."
  type        = string
  default     = "/aws/service/deeplearning/ami/x86_64/oss-nvidia-driver-gpu-pytorch-2.7-ubuntu-22.04/latest/ami-id"
}
