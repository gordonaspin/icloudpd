#!/bin/bash
set -euo pipefail
source .venv/bin/activate
scripts/build.sh
scripts/build.docker.local.sh
scripts/build.docker.repo.sh
