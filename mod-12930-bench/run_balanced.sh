#!/bin/bash
set -o pipefail
cd "$(dirname "$0")"
echo "=== balanced run started $(date) ==="
./.venv/bin/jupyter nbconvert --to notebook --execute --inplace mod12930_balanced.ipynb --ExecutePreprocessor.timeout=-1 \
  && echo "=== balanced run finished OK $(date) ===" \
  || echo "=== balanced run FAILED $(date) ==="
