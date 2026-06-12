#!/bin/sh
# Demo: a 3-node local cluster surviving node kills with zero correctness violations.
#
# Wraps the Phase 4 chaos harness (cmd/chaos, ADR 0026): starts a local etcd container if one
# isn't already reachable, then launches 3 cache-server nodes (RF=2), drives VERIFYING load
# (every fetched block is integrity-checked, ADR 0016), and hard-kills a random node every 15s.
# Exit 0 == zero violations.
#
# Usage:  make demo        (or: scripts/demo-chaos.sh [extra cmd/chaos flags])
# Needs:  Go 1.26+, and Docker only if no etcd is already on localhost:2379.

set -e
cd "$(dirname "$0")/.."

ETCD_ADDR="${ETCD_ADDR:-localhost:2379}"
ETCD_IMAGE="quay.io/coreos/etcd:v3.5.17"

etcd_up() {
  # etcd answers /health on its client port over HTTP.
  curl -fsS -m 2 "http://$ETCD_ADDR/health" >/dev/null 2>&1
}

if ! etcd_up; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "demo: no etcd on $ETCD_ADDR and no docker to start one." >&2
    echo "demo: start etcd yourself, e.g.:" >&2
    echo "  docker run -d --name kvc-etcd -p 2379:2379 $ETCD_IMAGE /usr/local/bin/etcd \\" >&2
    echo "    --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379" >&2
    exit 1
  fi
  echo "demo: starting local etcd container (kvc-etcd)..."
  docker start kvc-etcd >/dev/null 2>&1 || docker run -d --name kvc-etcd -p 2379:2379 \
    "$ETCD_IMAGE" /usr/local/bin/etcd \
    --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379 >/dev/null
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    etcd_up && break
    sleep 1
  done
  etcd_up || { echo "demo: etcd did not become healthy on $ETCD_ADDR" >&2; exit 1; }
fi

echo "demo: 3 nodes, RF=2, verified load; killing a node every 15s for 60s."
echo "demo: watch for 'kill' -> lease expiry -> ring rotation -> recovery. Exit 0 = 0 violations."
echo

exec go run ./cmd/chaos -etcd "$ETCD_ADDR" -nodes 3 -rf 2 \
  -duration 60s -kill-every 15s -down-time 8s "$@"
