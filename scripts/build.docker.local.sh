#!/bin/bash
set -euo pipefail

OWNER="gordonaspin"
PROJECT=$(basename $(pwd))
VERSION="$(cat pyproject.toml | grep version | cut -d'"' -f 2)"
REMOTE_HASH=$(date +%Y%m%d%H%M%S)
echo "Repo: ${OWNER}"
echo "Project: ${PROJECT}"
echo "Current ${PROJECT} version: ${VERSION}"
echo "Hash: ${REMOTE_HASH}"

docker build \
  --build-arg CACHE_BUST=${REMOTE_HASH} \
  --build-arg PROJECT=${PROJECT} \
  --progress plain \
  -t "${OWNER}/${PROJECT}:${VERSION}" \
  -f "Dockerfile.local" \
  .

docker tag "${OWNER}/${PROJECT}:${VERSION}" "${OWNER}/${PROJECT}:latest"
