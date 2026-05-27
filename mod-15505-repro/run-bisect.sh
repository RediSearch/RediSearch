#!/usr/bin/env bash
# MOD-15505 bisect runner: index-only ladder for a list of pre-built tags.
# Each tag is loaded into a single-node Redis container (--loadmodule) and
# benchmarked with load.py in 'index' mode.
#
# Usage: run-bisect.sh <out_csv> <tag1> [<tag2> ...]
#
# Env vars:
#   WORK_DIR  : where modules/ and data/ live (default: $PWD/mod-15505-work)
#   DATA_CSV  : path to MS MARCO slice (default: $WORK_DIR/data/msmarco-slice.csv)
#   N_DOCS    : docs per run (default: 30000)
#   N_RUNS    : runs per tag (default: 3)
#   N_THREADS : loader threads (default: 8)
#   MODE      : 'live' or 'index' (default: index)
#   PORT      : host port to expose Redis on (default: 16379)
set -euo pipefail

OUT_CSV="${1:?usage: run-bisect.sh <out_csv> <tag1> [<tag2> ...]}"
shift
TAGS=("$@")

WORK_DIR="${WORK_DIR:-$PWD/mod-15505-work}"
DATA_CSV="${DATA_CSV:-$WORK_DIR/data/msmarco-slice.csv}"
MODDIR="$WORK_DIR/modules"
N_DOCS="${N_DOCS:-30000}"
N_RUNS="${N_RUNS:-3}"
N_THREADS="${N_THREADS:-8}"
MODE="${MODE:-index}"
PORT="${PORT:-16379}"

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [ ! -f "$DATA_CSV" ]; then
  echo "!! DATA_CSV $DATA_CSV not found. Run fetch-data.sh first." >&2
  exit 1
fi

echo "label,run,docs,threads,send_s,total_s" > "$OUT_CSV"

# 2.10.x -> redis:7.4, 8.x -> redis:8.6, anything else (e.g. commit shas) -> redis:7.4.
img_for() {
  case "$1" in
    v2.10.*|2.10*) echo redis:7.4 ;;
    v8.*|8.*)      echo redis:8.6 ;;
    *)             echo redis:7.4 ;;
  esac
}

docker rm -f mod15505 >/dev/null 2>&1 || true

for tag in "${TAGS[@]}"; do
  so="$MODDIR/redisearch-${tag}.so"
  if [ ! -f "$so" ]; then
    echo "!! $tag: $so missing, skipping"
    continue
  fi
  img=$(img_for "$tag")
  echo "================================================================"
  echo "==> $tag  base=$img  N_DOCS=$N_DOCS  loader=$N_THREADS  mode=$MODE"
  echo "================================================================"
  docker rm -f mod15505 >/dev/null 2>&1 || true
  cid=$(docker run -d --rm --name mod15505 --cpus=8 -p ${PORT}:6379 \
          -v "$MODDIR":/m \
          "$img" \
          redis-server --loadmodule /m/redisearch-${tag}.so)
  for _ in $(seq 1 30); do
    if redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG; then break; fi
    sleep 0.3
  done
  mver=$(redis-cli -p "$PORT" MODULE LIST 2>/dev/null | awk '/^ver$/{getline; print; exit}')
  echo "  module-ver=$mver"
  for run in $(seq 1 "$N_RUNS"); do
    line=$(python3 "$HERE/load.py" "$DATA_CSV" "$N_DOCS" "$PORT" "$MODE" "$N_THREADS")
    echo "  run=$run  $line"
    send=$(echo "$line" | awk -F'send_s=' '{print $2}' | awk '{print $1}')
    total=$(echo "$line" | awk -F'total_s=' '{print $2}' | awk '{print $1}')
    echo "$tag,$run,$N_DOCS,$N_THREADS,$send,$total" >> "$OUT_CSV"
  done
  docker stop "$cid" >/dev/null 2>&1 || true
done

echo
echo "=== summary (median total_s per tag, in input order) ==="
python3 - "$OUT_CSV" "${TAGS[@]}" <<'PY'
import csv, statistics, sys
csvpath = sys.argv[1]
order = sys.argv[2:]
rows = list(csv.DictReader(open(csvpath)))
by = {}
for r in rows: by.setdefault(r["label"], []).append(float(r["total_s"]))
print(f"{'tag':<14}{'runs':<6}{'median_s':<10}{'min_s':<10}{'max_s':<10}{'vs_prev':<10}{'vs_first':<10}")
prev = None
first = None
for lbl in order:
    if lbl not in by: continue
    xs = sorted(by[lbl])
    med = statistics.median(xs)
    if first is None: first = med
    delta = f"{(med/prev-1)*100:+.1f}%" if prev else "-"
    dfirst = f"{(med/first-1)*100:+.1f}%" if first else "-"
    print(f"{lbl:<14}{len(xs):<6}{med:<10.3f}{xs[0]:<10.3f}{xs[-1]:<10.3f}{delta:<10}{dfirst:<10}")
    prev = med
PY
