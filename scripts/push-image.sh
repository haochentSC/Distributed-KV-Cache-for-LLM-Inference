#!/usr/bin/env bash
# Build the cache-server image (root Dockerfile) and push it to ECR.
#   scripts/push-image.sh <region> <image-ref>
# where <image-ref> is `terraform -chdir=terraform/cluster output -raw ecr_image`, e.g.
#   123456789012.dkr.ecr.us-east-1.amazonaws.com/kvcache/cache-server:latest
set -euo pipefail

REGION="${1:?usage: push-image.sh <region> <ecr-image-ref>}"
IMAGE="${2:?usage: push-image.sh <region> <ecr-image-ref>}"
REGISTRY="${IMAGE%%/*}" # everything before the first slash is the ECR registry host

# Build from the repo root (one level up from scripts/), where the Dockerfile lives.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

docker build -t "$IMAGE" "$ROOT"
docker push "$IMAGE"
echo "pushed $IMAGE"
