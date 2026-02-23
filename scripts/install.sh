#!/bin/bash
set -euo pipefail
source .venv/bin/activate
pip install dist/*.whl --force-reinstall
