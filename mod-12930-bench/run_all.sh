#!/bin/bash
set -o pipefail
cd "$(dirname "$0")"
echo "=== run started $(date) ===" 
./.venv/bin/jupyter nbconvert --to notebook --execute --inplace mod12930_benchmark.ipynb --ExecutePreprocessor.timeout=-1 \
  && ./.venv/bin/python make_report.py \
  && ./.venv/bin/python shot_report.py \
  && echo "=== run finished OK $(date) ===" \
  || echo "=== run FAILED $(date) ==="
