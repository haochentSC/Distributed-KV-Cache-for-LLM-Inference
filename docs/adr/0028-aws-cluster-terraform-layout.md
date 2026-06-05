# ADR 0028 — AWS cluster topology + Terraform module layout

- **Status:** accepted (authored; pending first `apply`)
- **Date:** 2026-06-05 (Phase 3/4 Sub-stage E)
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
- Not yet validated against a live account — `terraform fmt -check`/`validate` run as the first
  Stage-2 step once the toolchain is installed (Stage 0). The HCL is authored but unapplied; this
  ADR is "accepted (pending first apply)" and flips to plain "accepted" once the cluster stands up.
- CloudWatch **alarms** (vs just log groups) and the AWS-native **chaos scripts** (`tc`/`iptables`,
  instance-kill) are Stage 6, layered on this base.
