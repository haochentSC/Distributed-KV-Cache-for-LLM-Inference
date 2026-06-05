#!/usr/bin/env bash
# AWS chaos for the live cluster — the faults the local harness (cmd/chaos) can't do on Windows.
# Run `loadgen -verify` against the cluster (from a cache node) WHILE this runs, then assert zero
# correctness violations (ADR 0016) and watch recovery in Grafana.
#
#   scripts/aws-chaos.sh kill   <region>                 # terminate one random cache instance
#   scripts/aws-chaos.sh partition <cache-public-ip>     # iptables-drop etcd on a node for 30s
#   scripts/aws-chaos.sh latency   <cache-public-ip>     # tc adds 100ms to a node's egress for 30s
#
# 'partition'/'latency' SSH to the node and need an etcd peer IP / your key in the agent.
set -euo pipefail

mode="${1:?usage: aws-chaos.sh kill <region> | partition|latency <cache-public-ip>}"

case "$mode" in
kill)
  region="${2:?need region}"
  # Pick a random running cache instance by tag and terminate it (a real, unplanned node loss).
  ids=$(aws ec2 describe-instances --region "$region" \
    --filters "Name=tag:Name,Values=kvcache-cache-*" "Name=instance-state-name,Values=running" \
    --query "Reservations[].Instances[].InstanceId" --output text)
  if [ -z "$ids" ]; then
    echo "no running cache instances found" >&2
    exit 1
  fi
  victim=$(echo "$ids" | tr '\t' '\n' | shuf -n1)
  echo "terminating $victim — lease expires within ~lease-ttl, then failover to replica"
  aws ec2 terminate-instances --region "$region" --instance-ids "$victim" >/dev/null
  echo "terminated. Watch loadgen recover within the lease TTL; expect 0 violations."
  ;;

partition)
  ip="${2:?need a cache node public IP}"
  # Block this node's etcd traffic for 30s — it loses its lease and is dropped from the ring, then
  # rejoins. (Drops 2379/2380 egress; adjust if your etcd is elsewhere.)
  ssh "ec2-user@$ip" 'sudo iptables -A OUTPUT -p tcp --dport 2379 -j DROP; \
    sudo iptables -A OUTPUT -p tcp --dport 2380 -j DROP; \
    echo partitioned from etcd for 30s; sleep 30; \
    sudo iptables -D OUTPUT -p tcp --dport 2379 -j DROP; \
    sudo iptables -D OUTPUT -p tcp --dport 2380 -j DROP; echo healed'
  ;;

latency)
  ip="${2:?need a cache node public IP}"
  # Add 100ms egress latency for 30s (tc netem), then clear it.
  ssh "ec2-user@$ip" 'sudo tc qdisc add dev eth0 root netem delay 100ms; \
    echo "added 100ms egress latency for 30s"; sleep 30; \
    sudo tc qdisc del dev eth0 root netem; echo cleared'
  ;;

*)
  echo "unknown mode: $mode" >&2
  exit 1
  ;;
esac
