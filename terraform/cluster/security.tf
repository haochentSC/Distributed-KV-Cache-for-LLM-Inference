# Two security groups. The principle: gRPC + etcd traffic is allowed only WITHIN the cluster;
# SSH and the Prometheus metrics port are allowed only from your IP. Nothing else is open.

# --- cache nodes ---------------------------------------------------------------------------
resource "aws_security_group" "cache" {
  name        = "kvcache-cache"
  description = "cache nodes: gRPC intra-cluster; SSH+metrics from operator"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "kvcache-cache" }
}

# gRPC (50051) reachable from anything in the VPC: peer cache nodes (replication) and a future
# vLLM worker in the same VPC. Self-referencing + VPC CIDR keeps it inside the cluster.
resource "aws_vpc_security_group_ingress_rule" "cache_grpc" {
  security_group_id = aws_security_group.cache.id
  description       = "gRPC from within the VPC"
  cidr_ipv4         = var.vpc_cidr
  from_port         = 50051
  to_port           = 50051
  ip_protocol       = "tcp"
}

# Prometheus scrape (9090) only from the operator (your laptop running the local Prometheus).
resource "aws_vpc_security_group_ingress_rule" "cache_metrics" {
  security_group_id = aws_security_group.cache.id
  description       = "metrics from operator IP"
  cidr_ipv4         = var.my_ip_cidr
  from_port         = 9090
  to_port           = 9090
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "cache_ssh" {
  security_group_id = aws_security_group.cache.id
  description       = "SSH from operator IP"
  cidr_ipv4         = var.my_ip_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "cache_egress" {
  security_group_id = aws_security_group.cache.id
  description       = "all egress (ECR pull, S3, etcd, CloudWatch)"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

# --- etcd nodes ----------------------------------------------------------------------------
resource "aws_security_group" "etcd" {
  name        = "kvcache-etcd"
  description = "etcd: client 2379 from cache+operator, peer 2380 within the quorum"
  vpc_id      = aws_vpc.main.id
  tags        = { Name = "kvcache-etcd" }
}

# Client port 2379 from the cache nodes (they register + watch membership).
resource "aws_vpc_security_group_ingress_rule" "etcd_client_from_cache" {
  security_group_id            = aws_security_group.etcd.id
  description                  = "etcd client from cache nodes"
  referenced_security_group_id = aws_security_group.cache.id
  from_port                    = 2379
  to_port                      = 2379
  ip_protocol                  = "tcp"
}

# Client port 2379 from the operator (so you can run etcdctl / loadgen -etcd from your laptop).
resource "aws_vpc_security_group_ingress_rule" "etcd_client_from_operator" {
  security_group_id = aws_security_group.etcd.id
  description       = "etcd client from operator IP"
  cidr_ipv4         = var.my_ip_cidr
  from_port         = 2379
  to_port           = 2379
  ip_protocol       = "tcp"
}

# Peer port 2380 among etcd nodes themselves (Raft).
resource "aws_vpc_security_group_ingress_rule" "etcd_peer" {
  security_group_id            = aws_security_group.etcd.id
  description                  = "etcd peer (Raft) within the quorum"
  referenced_security_group_id = aws_security_group.etcd.id
  from_port                    = 2380
  to_port                      = 2380
  ip_protocol                  = "tcp"
}

resource "aws_vpc_security_group_ingress_rule" "etcd_ssh" {
  security_group_id = aws_security_group.etcd.id
  description       = "SSH from operator IP"
  cidr_ipv4         = var.my_ip_cidr
  from_port         = 22
  to_port           = 22
  ip_protocol       = "tcp"
}

resource "aws_vpc_security_group_egress_rule" "etcd_egress" {
  security_group_id = aws_security_group.etcd.id
  description       = "all egress"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}
