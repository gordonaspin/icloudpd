#!/bin/bash
set -euo pipefail

REPO="gordonaspin"
PROJECT=$(basename $(pwd))
VERSION="$(cat pyproject.toml | grep version | cut -d'"' -f 2)"
REMOTE_HASH=$(git ls-remote https://github.com/${REPO}/${PROJECT}.git HEAD | awk '{ print $1 }')
echo "Repo: ${REPO}"
echo "Project: ${PROJECT}"
echo "Current ${PROJECT} version: ${VERSION}"
echo "Git remote hash: ${REMOTE_HASH}"

docker build \
  --build-arg CACHE_BUST=${REMOTE_HASH} \
  --progress plain \
  -t "${REPO}/${PROJECT}:${VERSION}" \
  -f "Dockerfile" \
  .

docker tag "${REPO}/${PROJECT}:${VERSION}" "${REPO}/${PROJECT}:latest"
docker push -a ${REPO}/${PROJECT}
