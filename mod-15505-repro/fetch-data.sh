#!/usr/bin/env bash
# Download a slice of the public MS MARCO SETUP.csv used by the cloud
# ingest benchmark (MOD-15505 baseline). Full file is multi-GB; we only
# need ~90k docs (~700MB raw) for a ~50s/run index-only test.
#
# Usage: fetch-data.sh [size_mb]
#   size_mb: how many MB to pull (default 700, which yields ~89k docs)
#
# Env vars:
#   WORK_DIR : where data/ lives (default: $PWD/mod-15505-work)
set -euo pipefail

SIZE_MB="${1:-700}"
BYTES=$((SIZE_MB * 1024 * 1024))
WORK_DIR="${WORK_DIR:-$PWD/mod-15505-work}"
DATA_DIR="$WORK_DIR/data"
mkdir -p "$DATA_DIR"

URL='https://s3.amazonaws.com/benchmarks.redislabs/redisearch/datasets/6M-msmarco-documents/6M-msmarco-documents.redisearch.commands.SETUP.csv'
RAW="$DATA_DIR/msmarco-slice.raw.csv"
OUT="$DATA_DIR/msmarco-slice.csv"

if [ -f "$OUT" ] && [ -s "$OUT" ]; then
  rows=$(wc -l < "$OUT")
  echo "== $OUT already exists ($rows rows). Delete to re-download."
  exit 0
fi

echo "== fetching first ${SIZE_MB} MB of MS MARCO SETUP.csv (public S3, no creds)"
curl -sf -r "0-${BYTES}" "$URL" -o "$RAW"
ls -lh "$RAW"

echo "== trimming partial last line"
python3 - "$RAW" "$OUT" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
with open(src, 'rb') as f: data = f.read()
i = data.rfind(b'\n')
out = data[:i+1] if i >= 0 else data
open(dst, 'wb').write(out)
print(f"wrote {len(out)} bytes -> {dst}")
PY
rows=$(wc -l < "$OUT")
echo "== $OUT ready: ${rows} rows"
rm -f "$RAW"
