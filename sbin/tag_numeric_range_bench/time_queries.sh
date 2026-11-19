#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

# Times a tag filter combined with a numeric range of growing width on both indexes, then
# the other ways of expressing the same range on the index with unordered insertion.
# usage: time_queries.sh [repetitions per query]
set -uo pipefail
REPS=${1:-10}
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# The debug command is registered as _FT.DEBUG or FT.DEBUG depending on the build.
debug_cmd=_FT.DEBUG
$C "$debug_cmd" HELP 2>&1 | grep -qi 'unknown command' && debug_cmd=FT.DEBUG
for i in A B; do
  echo "numeric tree idx$i.ts: $($C "$debug_cmd" NUMIDX_SUMMARY "idx$i" ts | paste -sd' ')"
done

aggregate_usec() {
  $C INFO commandstats | tr -d '\r' | sed -n -E 's/^cmdstat_FT\.AGGREGATE:calls=[0-9]+,usec=([0-9]+).*/\1/p'
}

# timeit <label> <command...>: mean server-side time over REPS runs, and the last reply.
# Server-side time is used so that client and connection overhead stay out of the figure.
timeit() {
  local label=$1 before after reply i
  shift
  before=$(aggregate_usec); before=${before:-0}
  for ((i = 0; i < REPS; i++)); do reply=$($C "$@" | paste -sd' '); done
  after=$(aggregate_usec)
  printf '%-56s %9.3f ms   reply: %s\n' "$label" "$(echo "($after - $before) / $REPS / 1000" | bc -l)" "${reply:0:70}"
}

lo_ms() { echo $((HI_MS - ${WINDOW_SECS[$1]} * 1000)); }
COUNT=(GROUPBY 0 REDUCE COUNT 0 AS count)
WINDOWS=(5m 1h 3h 12h 24h)

echo; echo "== 1. Range in the query string =="
for i in A B; do
  for size in ${PLANTED//,/ }; do
    for w in "${WINDOWS[@]}"; do
      timeit "idx$i m$size window=$w" FT.AGGREGATE "idx$i" "@p:{m$size} @ts:[$(lo_ms "$w") $HI_MS]" DIALECT 2 "${COUNT[@]}"
    done
  done
done

largest=${PLANTED##*,}
lo24=$(lo_ms 24h)
echo; echo "== 2. Same 24h query on idxA m$largest with a flat union instead of a heap =="
heap_threshold=$($C FT.CONFIG GET UNION_ITERATOR_HEAP | tail -1)
$C FT.CONFIG SET UNION_ITERATOR_HEAP 1000000 >/dev/null
timeit "idxA m$largest window=24h flat union" FT.AGGREGATE idxA "@p:{m$largest} @ts:[$lo24 $HI_MS]" DIALECT 2 "${COUNT[@]}"
$C FT.CONFIG SET UNION_ITERATOR_HEAP "$heap_threshold" >/dev/null

echo; echo "== 3. Other ways to express the range, on idxA =="
for size in ${PLANTED//,/ }; do
  for w in 5m 24h; do
    lo=$(lo_ms "$w")
    timeit "m$size $w LOAD + FILTER" FT.AGGREGATE idxA "@p:{m$size}" DIALECT 2 LOAD 1 @ts FILTER "@ts >= $lo && @ts <= $HI_MS" "${COUNT[@]}"
    timeit "m$size $w FILTER on the SORTABLE field" FT.AGGREGATE idxA "@p:{m$size}" DIALECT 2 FILTER "@tss >= $lo && @tss <= $HI_MS" "${COUNT[@]}"
  done
  timeit "m$size all five windows in one query" FT.AGGREGATE idxA "@p:{m$size}" DIALECT 2 \
    FILTER "@tss >= $lo24 && @tss <= $HI_MS" \
    APPLY "@tss >= $(lo_ms 5m)" AS w5m APPLY "@tss >= $(lo_ms 1h)" AS w1h \
    APPLY "@tss >= $(lo_ms 3h)" AS w3h APPLY "@tss >= $(lo_ms 12h)" AS w12h \
    GROUPBY 0 REDUCE SUM 1 @w5m AS c5m REDUCE SUM 1 @w1h AS c1h REDUCE SUM 1 @w3h AS c3h \
    REDUCE SUM 1 @w12h AS c12h REDUCE COUNT 0 AS c24h
done

echo; echo "== 4. FT.PROFILE of the 24h query on idxA m$largest =="
$C FT.PROFILE idxA AGGREGATE LIMITED QUERY "@p:{m$largest} @ts:[$lo24 $HI_MS]" DIALECT 2 "${COUNT[@]}" | paste -sd' ' | cut -c1-1500
