# ADR 0028 — AWS cluster topology + Terraform module layout

- **Status:** accepted (first `apply` succeeded 2026-06-06; cluster verified live)
- **Date:** 2026-06-05 (Phase 3/4 Sub-stage E); first apply + verify 2026-06-06
- **Deciders:** HC (+ Claude)

## Context

The cluster has been local-first since Phase 2; this ADR records the actual AWS realization. The
topology was already decided across earlier ADRs (0004 Terraform, 0005 EC2-not-EKS, 0006 Spot
cache, 0009 on-demand 3-node etcd, 0023 Spot drain, 0027 cold tier). What was undecided was the
*shape of the Terraform* — module split, how etcd forms a quorum without a discovery service, how
the image reaches the nodes, and how the running flags get wired — and the network/IAM specifics.

## Decision

**Two Terraform roots under `terraform/`:**
- **`bootstrap/`** — the S3 state bucket + DynamoDB lock table, kept on **local state** (it can't
  store the backend that defines itself). Applied once.
- **`cluster/`** — everything else, on the S3 remote backend, as a flat root of small files
  (`network`, `security`, `iam`, `ecr`, `s3`, `cloudwatch`, `etcd`, `cache`) rather than nested
  modules. At this size a flat root is easier to read and `apply` than premature module
  abstraction; modularization is a later refinement if a second environment appears.

**Network:** one VPC, **one public subnet, single AZ** (low-latency intra-cluster gRPC). Public IPs
+ IGW and **no NAT gateway** — NAT is the one line item that would cost real money here, and the
nodes only need outbound (ECR/S3/CloudWatch), which the IGW gives. SSH (22) and metrics (9090) are
opened **only to the operator's `/32`**; gRPC (50051) only within the VPC; etcd client/peer
(2379/2380) only from the cache SG / within the etcd SG.

**etcd quorum via static bootstrap.** The three etcd nodes get **fixed private IPs** (`.10/.11/.12`),
so user-data can template the full `--initial-cluster` string *before the instances exist* — no
discovery service, no coordination dance. This is the cleanest way to stand up a fixed-size quorum.

**Cache delivery: ECR image under systemd-managed Docker.** user-data installs Docker, `ecr
get-login`, and writes a systemd unit that runs the image with `--network host`. The flags
(`-etcd`, `-advertise <private-ip>`, `-node-id <instance-id>`, `-spot`, `-max-bytes`,
`-cold-bucket`, …) are assembled by a small runtime wrapper so the instance id / private IP are
resolved at service start via IMDSv2. **`ExecStop` = `docker stop`** sends SIGTERM into the
container → the deregister-then-drain path (ADR 0023). Cache nodes are **individual `aws_instance`s,
not an ASG** (ADR 0005): the chaos harness terminates one cleanly without a controller racing to
replace it.

**IMDSv2 hop limit 2 on cache nodes.** The Spot poller (`internal/spot`) runs *inside the
container*, one network hop further from the metadata service than the host; hop-limit 2 lets it
reach IMDS. etcd nodes keep hop-limit 1 (no container).

**Least-privilege IAM, no static creds.** Two roles/instance-profiles: cache (ECR read via the
managed read-only policy, CloudWatch logs, S3 cold-tier `Get/Put/List` scoped to the cold bucket
ARN only) and etcd (CloudWatch logs). Credentials come from the instance role; nothing static is
ever written.

## Why not these alternatives

- **etcd discovery (DNS/etcd-discovery URL) instead of static IPs.** Overkill for a fixed 3-node
  cluster and adds a moving part; static private IPs make the initial-cluster string a pure
  function of config.
- **An Auto Scaling Group for cache nodes.** A controller that replaces a terminated node fights
  the chaos harness and contaminates recovery measurements (the ADR 0005 reasoning). Discrete
  instances keep failures clean.
- **Raw binary fetched from S3 instead of ECR+Docker.** Simpler, but ECR+Docker is the named
  deliverable and the stronger cloud-native signal; the Dockerfile already exists.
- **NAT gateway + private subnets.** More "correct" for production, but it adds hourly cost and
  buys nothing for a dev/benchmark cluster whose ingress is already locked to one IP.
- **Nested Terraform modules now.** Premature; a flat root is clearer until there's a second
  environment to factor out.

## Consequences

- The whole cluster is `apply`/`destroy`-reproducible, which is what makes chaos runs repeatable
  (rebuild a clean cluster between experiments).
- **loadgen must run inside the VPC** (cache nodes advertise private IPs): copy a Linux build to a
  cache node, or use a bastion. Documented in `terraform/README.md`.
- CloudWatch **alarms** (vs just log groups) and the AWS-native **chaos scripts** (`tc`/`iptables`,
  instance-kill) are Stage 6, layered on this base.

## First-apply findings (2026-06-06)

The cluster stood up and was verified live (etcd 3-node quorum with an elected leader; all 3 Spot
cache nodes registered under `/kvcache/members/`; a `loadgen -verify` run inside the VPC reported
**0 correctness violations** over 6.6k requests and reproduced the ADR 0014 ~87% hot-shard
concentration at prefix-share 0.8). Three fixes were needed along the way:

- **`fmt`/`validate` cleanups.** `s3.tf` needed `terraform fmt`; `cache.sh.tftpl` had a literal
  `${...}` inside a comment that `templatefile()` tried to parse as an interpolation (it parses the
  whole file, comments included) — reworded.
- **First-boot image race (real bug, not bad luck).** In this deploy model the image push *always*
  follows the cluster `apply` (the ECR repo must exist first), so a fresh cache node boots before
  the image exists. The old user-data ran `docker pull` under `set -euo pipefail`, so the missing
  image **aborted user-data before the systemd unit was even written** — the node then couldn't
  self-heal. Fix: the boot-time pull is now non-fatal; `cache-server.service` (`Restart=always`)
  retries the pull via `docker run` until the image lands. Added `user_data_replace_on_change =
  true` so template fixes actually recreate the node on apply.
- **`t3.small` CPU-credit exhaustion (sizing).** `t3.*` are burstable (~0.2 vCPU baseline). A load
  test driven *on* a cache node depleted the credit balance to 0; AWS then throttled the box to
  baseline, which starved the 10s etcd lease keepalive and dropped every node from the ring (empty
  `/kvcache/members/`) and even blocked `sshd`. Mitigated by switching cache nodes to
  `credit_specification { cpu_credits = "unlimited" }` (in the Terraform now). For sustained
  benchmark/chaos load a **non-burstable** type (e.g. `c7i.large`) is the better substrate; also
  drive `loadgen` from an etcd node or a bastion, not from a shard. `t3.small`'s 2 GiB RAM is also
  tight against the 1.5 GB `cache_max_bytes` default — size up for real benchmarks.
- **To verify:** whether a cache-server re-registers on its own after an etcd lease *lapse* (here
  recovery took an instance reboot, but that was confounded by the credit throttle — isolate under
  Sub-stage C failover testing).
