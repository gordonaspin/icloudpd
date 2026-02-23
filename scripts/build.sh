#!/bin/bash
set -euo pipefail
rm -f dist/*
source .venv/bin/activate
python -m pylint src
python -m build
