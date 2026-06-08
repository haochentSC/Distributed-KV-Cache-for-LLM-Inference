# Terraform — KV cache on AWS

Stands up the cluster from the plan §Phase 3/4 Sub-stage E: a single-AZ VPC, a 3-node on-demand
etcd quorum (ADR 0009), N Spot cache nodes running the ECR image (ADR 0006), an S3 cold tier
(ADR 0027), least-privilege IAM (no static creds, ADR 0004), and CloudWatch logs.

> **Never `terraform apply` is run for you.** Claude shows `plan`; **you** run `apply`/`destroy`
> (`.claude/rules/terraform.md`). Topology decisions are ADR-locked — see `docs/05-cloud-deployment-aws.md`.

```
bootstrap/   S3 state bucket + DynamoDB lock  (LOCAL state; apply once)
cluster/     the VPC, etcd, cache nodes, ECR, S3 cold tier, IAM, CloudWatch  (S3 remote state)
  userdata/  etcd + cache cloud-init templates
```

## Stage 0 — one-time AWS onboarding (manual)

1. Create an AWS account; enable **MFA on root**; then create an **IAM admin user** (don't use root day-to-day).
2. Install the **AWS CLI v2** and **Terraform ≥ 1.6**.
3. `aws configure` with the admin user's access keys. Verify: `aws sts get-caller-identity`.
4. Set a budget alarm so a mistake can't run away:
   `aws budgets create-budget` (or the console → Billing → Budgets) — e.g. $20/month.
5. `terraform version` works.

## Stage 1 — remote-state backend (apply once)

```
cd terraform/bootstrap
terraform init
terraform apply -var state_bucket=kvcache-tfstate-<your-account-id>
terraform output           # note state_bucket + lock_table
```

## Stage 2–4 — the cluster

```
cd ../cluster
cp backend.hcl.example backend.hcl        # fill in bucket/table/region from Stage 1
cp terraform.tfvars.example terraform.tfvars   # set my_ip_cidr (curl -s https://checkip.amazonaws.com), key_name, sizes
terraform init -backend-config=backend.hcl
terraform fmt -check && terraform validate
terraform apply                            # review the plan, then confirm

# Build + push the image, then the cache nodes can pull it:
../../scripts/push-image.sh us-east-1 "$(terraform output -raw ecr_image)"
# (If the cache nodes booted before the image existed, they retry via systemd — or just
#  re-pull on a node: `sudo systemctl restart cache-server`.)
```

### Verify
- etcd: `ssh ec2-user@<etcd_public_ip>` then `etcdctl member list -w table` → 3 **started** members.
- membership: `etcdctl get --prefix /kvcache/members/` → one key per cache node.
- traffic: loadgen must run **inside the VPC** (cache nodes advertise private IPs). Build it for
  Linux and copy it to a cache node, then run against the **private** etcd endpoints:
  `GOOS=linux GOARCH=amd64 go build -o loadgen ./cmd/loadgen && scp loadgen ec2-user@<cache_public_ip>:`
  then on the node: `./loadgen -etcd "$(terraform output -raw etcd_client_endpoints_private)" -verify -duration 60s -payload-bytes 262144`.
- metrics: add `terraform output prometheus_targets` to `deploy/observability/prometheus.yml`
  and bring up the local Grafana stack.
- cold tier: run with a low `-max-bytes` to force eviction, then `aws s3 ls s3://<cold_bucket>/blocks/ --recursive`
  → objects appear; a later Fetch for an evicted block is a recovered cold hit (0 violations).

## Operational notes (learned from the first live apply, 2026-06-06)

- **Image push *follows* the cluster apply.** The ECR repo is created by `apply`, so cache nodes
  boot before any image exists. User-data's `docker pull` is non-fatal and `cache-server.service`
  retries until you run `push-image.sh` — so a fresh cluster has etcd healthy but cache containers
  retrying until the push lands. That's expected, not a failure.
- **Drive `loadgen` from an etcd node or a bastion, never from a cache node.** loadgen would compete
  with the cache-server for the same 2 vCPUs. It still must run *inside the VPC* (nodes advertise
  private IPs). And keep the run gentle on small instances (low `-concurrency`, smaller
  `-payload-bytes`).
- **Instance sizing — `t3.small` is borderline.** `t3.*` are burstable; under a real load test the
  CPU-credit balance hits 0 and AWS throttles the node to ~0.2 vCPU, which starves the 10s etcd lease
  keepalive and **drops the node from the ring** (empty `/kvcache/members/`) — and even blocks SSH.
  The Terraform now sets `cpu_credits = "unlimited"` on cache nodes to avoid this. For sustained
  benchmark/chaos load, set `cache_instance_type` to a **non-burstable** type (e.g. `c7i.large`);
  also note `t3.small`'s 2 GiB RAM is tight against the 1.5 GB `cache_max_bytes` default.
- **`etcdctl endpoint health --cluster` shows 2/3 unhealthy — that's the SG, not a real fault.** The
  etcd SG allows client port 2379 only from cache nodes + operator; peers use 2380. Confirm quorum
  with a `put`/`get` (which needs quorum) instead.
- **Windows/PowerShell:** quote the backend arg (`terraform init "-backend-config=backend.hcl"`) or
  PowerShell splits on `=`; winget-installed `terraform`/`aws` need a new shell (or a registry PATH
  refresh) to resolve; `ssh -F NUL` bypasses a `~/.ssh/config` with bad perms.

## Teardown

```
cd terraform/cluster && terraform destroy
```
Leave `bootstrap/` up (it holds state). The cold bucket and ECR repo have `force_destroy`/`force_delete`
so destroy is clean. Tear down between experiments — rebuilding from `apply` is the point of IaC.
