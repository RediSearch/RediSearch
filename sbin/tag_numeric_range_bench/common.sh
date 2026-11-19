#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

# Shared setup, sourced by the scripts in this directory: starts a throwaway redis-server
# with the module, creates the two indexes and loads the dataset.
#
# Environment:
#   NUM_DOCS  documents per index (default 2000000)
#   PLANTED   sizes of the planted tag values, comma separated (default 500,2000,5000,20000)
#   MODULE    path to redisearch.so (default: the release build under bin/)
#
# Leaves C (the redis-cli command line), PORT, HI_MS and WINDOW_SECS for the caller. The
# server is stopped when the calling script exits; a caller with more to clean up
# redefines cleanup().

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
NUM_DOCS=${NUM_DOCS:-2000000}
PLANTED=${PLANTED:-500,2000,5000,20000}
MODULE=${MODULE:-$(ls "$ROOT"/bin/linux-*-release/search-community/redisearch.so 2>/dev/null | head -1)}
[ -f "$MODULE" ] || { echo "no release module found: build one with ./build.sh or set MODULE" >&2; exit 1; }

# Timestamps are uniform over SPAN_MS starting at T0_MS. Every query window ends at HI_MS,
# which is far enough from both ends for the widest window to stay inside the data.
T0_MS=1713040000000
SPAN_MS=$((48 * 3600 * 1000))
HI_MS=$((T0_MS + 40 * 3600 * 1000))
declare -A WINDOW_SECS=([5m]=300 [1h]=3600 [3h]=10800 [12h]=43200 [24h]=86400)

RSV="$ROOT/.skills/drive-redisearch/rsv.sh --name tag-numeric-range-bench"
$RSV stop >/dev/null 2>&1
$RSV start --module "$MODULE" 2>&1 | grep -E 'up on|FAIL' || exit 1
cleanup() { $RSV stop >/dev/null 2>&1; }
trap cleanup EXIT
PORT=$($RSV port)
C="redis-cli -p $PORT"

# idxA covers a:* (inserted in an order unrelated to the timestamp) and idxB covers b:*
# (inserted in timestamp order). ts and tss hold the same value; only tss is SORTABLE.
for x in A:a B:b; do
  $C FT.CREATE "idx${x%%:*}" ON HASH PREFIX 1 "${x##*:}:" SCHEMA p TAG e TAG ts NUMERIC tss NUMERIC SORTABLE >/dev/null
done
load_start=$(date +%s)
python3 "$HERE/gen_dataset.py" "$NUM_DOCS" "$PLANTED" "$T0_MS" "$SPAN_MS" | $C --pipe 2>&1 | tail -1
echo "loaded in $(( $(date +%s) - load_start ))s"
