# AWS paid-batch runbook — distributed GPU TTFT + on-cluster validations

> **EXECUTED 2026-06-10/11 — results + findings in [`phase45-distributed-gpu.md`](phase45-distributed-gpu.md).**
> Highlights vs this plan: g5.2xlarge had zero Spot capacity region-wide → ran on **g6.2xlarge (1× L4)**
> via the new `gpu_az`/`aws_subnet.gpu` (capacity is AZ-dependent — check `aws ec2
> get-spot-placement-scores` first); c7i.large was reclaimed-for-capacity → t3.small + the
> `AWS_REGION`/`GOMEMLIMIT`/1.0 GB fixes held up fine; the TTFT crossover landed at ~1k tokens
> (+10.9 % at 4k). Keep the steps below for a re-run.

The single paid window that produces the headline `[measured]` TTFT number (ADR 0031/0032) and
clears the remaining on-cluster validations. Everything here was prepped and locally verified
beforehand; this window is **execution only**, kept short to limit spend.

> **The GPU node bills hourly.** Bring it up only for the run and tear it down immediately after
> (`-var gpu_count=0` or `terraform destroy`). `gpu_count` defaults to 0, so it is never created by
> a normal apply.

## Account prerequisites & GPU scope (read first)

Two AWS account gates must clear before any GPU launch (discovered 2026-06-09, ADR 0033):

1. **Paid Plan.** A new account on the Free Plan only launches free-tier-eligible types and rejects
   GPU (`InvalidParameterCombination: not eligible for Free Tier`). Upgrade in Billing → Account.
2. **G/VT Spot vCPU quota** (`L-3819A6DF`, us-east-1). New accounts start at 0; the standard GPU
   approval ceiling is **8 vCPUs** (beyond that AWS routes you to sales). 8 vCPUs = one single-GPU box.

**Default scope = single-GPU (the 8-vCPU path).** `gpu_instance_type` defaults to `g5.2xlarge`
(1× A10G, 24 GB); run a 7-8B bf16 (or ~13-14B quantized) model at `--tensor-parallel-size 1`. This is
the pinned-memory, full-KV-budget environment ADR 0031 predicted would flip the cache to net-positive —
the legitimate single-GPU headline.

**TP=4 / 30B is deferred (Option B):** needs 48 vCPUs via AWS sales, or more practically a
GPU-specialized cloud (Lambda/RunPod/Modal) — the connector's TP keying (ADR 0032) is provider-agnostic.
Use the `g5.12xlarge` + `--tensor-parallel-size 4` variants below only on that path.

## Rough cost

Dominated by the GPU. The default **`g5.2xlarge`** (1× A10G) ≈ **$1.21/hr on-demand, ~$0.45–0.70/hr
Spot**. (Deferred TP path: `g5.12xlarge` (4× A10G) ≈ $5.67/hr on-demand, ~$2–3/hr Spot.)
+ 120 GB gp3 (~$0.02/hr) + 3× etcd `t3.small` on-demand (~$0.06/hr total) + 3× cache `t3.small` Spot
(negligible) + minimal S3/CloudWatch. A focused **2–3 hr** single-GPU window ≈ **$2–6**. Model weights
download from Hugging Face is inbound (free). Budget alarm from `terraform/README.md` Stage 0 still applies.

## Pre-flight (no spend; mostly already done)

- `go build ./...`, `go test ./...` green; `gofmt`/`go vet` clean. `-race` runs in **WSL2** only
  (32-bit MinGW cgo blocks it on Windows) — run `go test ./... -race` there if touching Go.
- `cd terraform/cluster && terraform fmt -check && terraform validate` pass.
- Confirm the spend guard: `terraform plan` with no GPU var shows **no** `aws_instance.gpu` /
  `aws_security_group.gpu` / `data.aws_ssm_parameter.dlami`. `terraform plan -var gpu_count=1` shows
  exactly one GPU instance + its SG.
- Connector TP unit test: `cd connector && python -m pytest tests/test_hashing.py -q`.
- Decide the model: default single-GPU path is a 7-8B (e.g. `Qwen/Qwen2.5-7B-Instruct`) that fits one
  A10G in bf16. (Deferred TP=4 path only: confirm the 30B model's `num_key_value_heads` is divisible
  by `tensor_parallel_size=4`.)

## Step 1 — cluster up + image pushed

```
cd terraform/cluster
terraform init -backend-config=backend.hcl     # if not already initialized
terraform apply                                 # 3 etcd + 3 cache (no GPU yet)
../../scripts/push-image.sh us-east-1 "$(terraform output -raw ecr_image)"
```
Verify per `terraform/README.md` → Verify: etcd quorum, `/kvcache/members/` populated, a gentle
`loadgen -verify` reports **0 violations**. For sustained load consider `cache_instance_type =
c7i.large` (non-burstable; ADR 0028 t3 credit-throttle finding).

## Step 2 — GPU node up + environment

```
terraform apply -var gpu_count=1
terraform output gpu_public_ips        # SSH target
terraform output cache_private_ips     # the connector's --cache-addr target (VPC-internal)
```
If `apply` errors resolving the Deep Learning AMI, the SSM path has drifted — find a current DLAMI
(PyTorch, GPU) id in the console and re-apply with `-var gpu_count=1 -var gpu_ami_id=ami-xxxx`.

On the GPU node (DLAMI ships CUDA + PyTorch):
```
ssh ubuntu@<gpu_public_ip>        # DLAMI default user is 'ubuntu' (ec2-user on some AMIs)
pip install vllm==0.22.1          # pin the version the connector + probe were written against
# copy the connector package over (from the laptop), then:
pip install -e distributed-kv-cache/connector
nvidia-smi                        # confirm the GPU is visible (1 on g5.2xlarge; 4 on g5.12xlarge)
```

## Step 3 — probe the KV layout (BEFORE any real run)

```
python connector/tools/probe_kv_layout.py \
  --model Qwen/Qwen2.5-7B-Instruct --out kv_layout_probe.json \
  --gpu-mem-util 0.90
```
In the dump, confirm `block_axis` is unchanged and `tp_rank`/`tp_world` recorded (ADR 0032). On the
single-GPU default `tp_world=1` (no sharding). (Deferred TP=4 path only: run with
`tensor_parallel_size=4` and also confirm per-rank KV heads = `num_kv_heads / 4`.) This is the gate
before spending on the benchmark.

## Step 4 — the headline: distributed TTFT (single-GPU 7-8B; default path)

```
python connector/scripts/run_distributed_benchmark.py \
  --models Qwen/Qwen2.5-7B-Instruct \
  --tensor-parallel-size 1 \
  --cache-addr <cache_private_ip>:50051 \
  --workload system_prompt --repeats 4,8,16 \
  --gpu-mem-util 0.90 \
  --output docs/benchmarks/phase45-distributed-qwen7b.json
```
`warm_vs_baseline_pct > 0` = the cache wins. For the scaling-curve artifact, pass several `--models`
(e.g. 7B + a quantized 13-14B) in one invocation. Watch for `[kvc] load path active` / `save path
active` and zero correctness warnings. **(Deferred TP path:** `--models Qwen/Qwen2.5-32B-Instruct
--tensor-parallel-size 4` on a `g5.12xlarge` or a GPU-specialized cloud.)

## Step 5 — on-cluster validations (CPU; cheap, run while the cluster is up)

> **Lesson from 2026-06-09:** on `t3.small` cache nodes, running `loadgen` *on* the node under load
> (or via `-etcd` discovery, which spreads load to all three) exhausts CPU credits and makes the node
> SSH-unreachable mid-test. Run `loadgen` from a **peer cache node** (cross-compile
> `GOOS=linux GOARCH=amd64 go build ./cmd/loadgen`, `scp` it over) and **pin it at a different node**
> with `-members <other-node-private-ip>:50051`, so generator and server-under-test never share a box.
> Do **not** run load from an etcd node (risks dropping quorum). Better: bring the cluster up with
> `-var cache_instance_type=c7i.large` (non-burstable; same 2 vCPU, no new quota) for this step.

- **Chaos** (`scripts/aws-chaos.sh`), with `loadgen -verify -duration 120s` running from a peer cache
  node (per the lesson above): `kill <region>` (terminate a cache node), `partition <cache-public-ip>`,
  `latency <cache-public-ip>`. Assert **0 violations** through each; watch recovery in Grafana.
  The `kill` + **CloudWatch alarm transition** is throttle-immune and works even on `t3.small`.
- **Cold-tier round-trip** — point at ONE cache node so the working set lands on it:
  ```
  ./loadgen -members <one-cache-private-ip>:50051 -verify-coldtier \
    -coldtier-blocks 8000 -payload-bytes 262144 -coldtier-settle 5s
  aws s3 ls s3://$(terraform output -raw cold_bucket)/blocks/ --recursive | head
  ```
  Size `coldtier-blocks*payload-bytes` to comfortably exceed the node's `cache_max_bytes` (8000 ×
  256 KiB ≈ 2 GiB > 1.5 GiB default). Expect `PASS: every block read back` + objects in S3.
- **CloudWatch alarms** fire: stop a node (or use the chaos kill) and confirm the
  `kvcache-*-unhealthy` alarm transitions in the console (SNS email if `alarm_email` was set).
- **Eviction 5a/5b re-run on the cluster**: `loadgen -multitenant` against a node running
  `-eviction gdsf` / `-eviction gdsf-elastic -fairness-weight w` and `-tenant-quota …`, sweeping `w`
  as in `scripts/phase5b-sweep.ps1`. Confirm the cluster numbers track the local Pareto frontier.

## Step 6 — capture + TEARDOWN

- Commit the captured JSON under `docs/benchmarks/`; fill the resume `[measured]` from Step 4 (or
  record the scaling curve if still transport-bound — the honest ADR 0031 framing).
- **Tear down the GPU first**, then the cluster:
  ```
  terraform apply -var gpu_count=0        # drop just the GPU node (keeps the cluster), OR:
  terraform destroy                        # tear the whole cluster down
  ```
- Sanity: `aws ec2 describe-instances --filters Name=tag:Name,Values=kvcache-gpu-* \
  Name=instance-state-name,Values=running` returns nothing. Leave `bootstrap/` (it holds state).
