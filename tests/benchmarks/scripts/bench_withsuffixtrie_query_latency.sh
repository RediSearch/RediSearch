#!/usr/bin/env bash
# End-to-end FT.SEARCH latency for the suffix-trie query shapes, to see whether
# the contains micro-step gap (Rust ~1.36x C on the iterate sub-step) is visible
# in full query latency.
#
# Usage:  bench_withsuffixtrie_query_latency.sh [path/to/redisearch.so]
#   defaults to the release build under ./bin
# Run it once per build (this branch's .so vs a master .so) to A/B C vs Rust.
# For production-representative numbers, run on Linux with an LTO build.
set -euo pipefail

MODULE="${1:-$(find bin -name redisearch.so -path '*release*' | head -1)}"
MODULE="$(cd "$(dirname "$MODULE")" && pwd)/$(basename "$MODULE")"
PORT="${PORT:-7799}"
NDOCS="${NDOCS:-30000}"
NQ="${NQ:-50000}"
DATADIR="$(mktemp -d "${TMPDIR:-/tmp}/ftbench.XXXXXX")"

echo "Module : $MODULE"
echo "Docs   : $NDOCS   Queries/run: $NQ   Port: $PORT"

redis-server --port "$PORT" --save '' --appendonly no --dir "$DATADIR" \
  --loadmodule "$MODULE" >/dev/null 2>&1 &
SRV=$!
trap 'kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; rm -rf "$DATADIR"' EXIT

for _ in $(seq 1 100); do
  [ "$(redis-cli -p "$PORT" ping 2>/dev/null)" = PONG ] && break; sleep 0.1
done

redis-cli -p "$PORT" FT.CONFIG SET DEFAULT_DIALECT 2 >/dev/null
redis-cli -p "$PORT" FT.CREATE idx ON HASH SCHEMA text TEXT WITHSUFFIXTRIE >/dev/null

# Corpus: words over an 8-letter alphabet so 2-3 grams recur and infix queries
# match many terms (a realistic broad-infix workload). Deterministic.
python3 - "$NDOCS" <<'PY' | redis-cli -p "$PORT" --pipe
import sys, random
n = int(sys.argv[1]); random.seed(42)
alpha = "abcdefgh"
def word(): return "".join(random.choice(alpha) for _ in range(random.randint(4, 8)))
def resp(*a):
    out = ["*%d\r\n" % len(a)]
    for x in a:
        x = str(x); out.append("$%d\r\n%s\r\n" % (len(x), x))
    return "".join(out)
w = sys.stdout.write
for i in range(n):
    w(resp("HSET", "doc:%d" % i, "text", " ".join(word() for _ in range(6))))
PY

# Wait for indexing to drain.
for _ in $(seq 1 200); do
  ind="$(redis-cli -p "$PORT" FT.INFO idx | awk '/percent_indexed/{getline; print}')"
  [ "$ind" = "1" ] && break; sleep 0.1
done
echo "Indexed: num_docs=$(redis-cli -p "$PORT" FT.INFO idx | awk '/num_docs/{getline; print}')"
echo

bench() { # label query
  printf '%-26s ' "$1"
  redis-benchmark -p "$PORT" -n "$NQ" -c 1 -q FT.SEARCH idx "$2" NOCONTENT LIMIT 0 0 \
    | sed 's/^FT.SEARCH[^:]*: //'
}

echo "=== query latency (single client, p50) ==="
bench "prefix   ab*  (baseline)" 'ab*'
bench "suffix   *ab  (ends-with)" '*ab'
bench "contains *ab* (infix)"     '*ab*'
bench "contains *cde* (narrower)" '*cde*'
