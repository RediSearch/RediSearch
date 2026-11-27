#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

# Samples where the time goes while the 24h query runs back to back, once per index.
# usage: profile_queries.sh [planted tag size] [seconds of sampling per index] [output dir]
set -uo pipefail
TAG_SIZE=${1:-5000}
SECS=${2:-60}
OUT=${3:-profile-out}
for tool in gdb redis-benchmark; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
mkdir -p "$OUT"
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
case ",$PLANTED," in *",$TAG_SIZE,"*) ;; *) echo "tag size $TAG_SIZE is not one of PLANTED=$PLANTED" >&2; exit 1 ;; esac

bench_pid=
cleanup() {
  [ -n "$bench_pid" ] && kill "$bench_pid" 2>/dev/null
  $RSV stop >/dev/null 2>&1
}
server_pid=$($C INFO server | tr -d '\r' | sed -n 's/^process_id://p')
lo_ms=$((HI_MS - ${WINDOW_SECS[24h]} * 1000))

for i in A B; do
  query=(FT.AGGREGATE "idx$i" "@p:{m$TAG_SIZE} @ts:[$lo_ms $HI_MS]" DIALECT 2 GROUPBY 0 REDUCE COUNT 0 AS count)
  echo; echo "== idx$i reply: $($C "${query[@]}" | paste -sd' ')"
  $C FT.PROFILE "idx$i" AGGREGATE LIMITED QUERY "${query[2]}" "${query[@]:3}" | paste -sd' ' | sed -E 's/ +/ /g' | cut -c1-900
  # One connection, so exactly one query runs at a time and every sample of the query
  # thread sees the same workload.
  redis-benchmark -p "$PORT" -c 1 -n 100000000 -q "${query[@]}" >/dev/null 2>&1 &
  bench_pid=$!
  sleep 1
  SAMPLE_OUT="$OUT/stacks_$i.txt" SAMPLE_SECS="$SECS" \
    gdb -nx -q -batch -iex 'set debuginfod enabled off' -p "$server_pid" -x "$HERE/gdb_sampler.py" >/dev/null 2>&1
  kill "$bench_pid" 2>/dev/null
  wait "$bench_pid" 2>/dev/null
  bench_pid=
  python3 "$HERE/analyze_stacks.py" "$OUT/stacks_$i.txt" "$OUT/folded_$i.txt"
done
