#!/bin/bash
set -euo pipefail

OWNER="gordonaspin"
PROJECT=$(basename $(pwd))
VERSION="$(cat pyproject.toml | grep version | cut -d'"' -f 2)"
REMOTE_HASH=$(git ls-remote https://github.com/${OWNER}/${PROJECT}.git HEAD | awk '{ print $1 }')
echo "Repo: ${OWNER}"
echo "Project: ${PROJECT}"
echo "Current ${PROJECT} version: ${VERSION}"
echo "Git remote hash: ${REMOTE_HASH}"

docker build \
  --build-arg CACHE_BUST=${REMOTE_HASH} \
  --build-arg OWNER=https://github.com/${OWNER}/${PROJECT}.git \
  --build-arg PROJECT=${PROJECT} \
  --progress plain \
  -t "${OWNER}/${PROJECT}:${VERSION}" \
  -f "Dockerfile" \
  .

docker tag "${OWNER}/${PROJECT}:${VERSION}" "${OWNER}/${PROJECT}:latest"
docker push -a ${OWNER}/${PROJECT}
