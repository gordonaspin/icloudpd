#!/bin/bash
set -euo pipefail

REPO="gordonaspin"
PROJECT=$(basename $(pwd))
ICLOUDPD_VERSION="$(cat pyproject.toml | grep version | cut -d'"' -f 2)"
REMOTE_HASH=$(date +%Y%m%d%H%M%S)
echo "Repo: ${REPO}"
echo "Project: ${PROJECT}"
echo "Current ${PROJECT} version: ${ICLOUDPD_VERSION}"
echo "Hash: ${REMOTE_HASH}"

docker build \
  --build-arg CACHE_BUST=${REMOTE_HASH} \
  --progress plain \
  -t "${REPO}/${PROJECT}:${ICLOUDPD_VERSION}" \
  -f "Dockerfile.local" \
  .

docker tag "${REPO}/${PROJECT}:${ICLOUDPD_VERSION}" "${REPO}/${PROJECT}:latest"
