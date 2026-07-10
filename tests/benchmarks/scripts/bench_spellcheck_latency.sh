#!/usr/bin/env bash
# End-to-end latency for the spell-check dictionary command surface, to see
# whether the micro-step gaps (Rust contains ~282x, fuzzy ~36x C at 10k terms;
# Rust wins insert/dump/RDB-save) are visible through the real command path:
# FT.SPELLCHECK with include/exclude dicts, FT.DICTDUMP, and DEBUG RELOAD
# (aux RDB save+load).
#
# Usage:  bench_spellcheck_latency.sh [path/to/redisearch.so]
#   defaults to the release build under ./bin
# Run it once per build (this branch's .so vs a pre-swap .so) to A/B Rust vs C.
# For production-representative numbers, run on Linux with an LTO build.
set -euo pipefail

MODULE="${1:-$(find bin -name redisearch.so -path '*release*' | head -1)}"
MODULE="$(cd "$(dirname "$MODULE")" && pwd)/$(basename "$MODULE")"
PORT="${PORT:-7799}"
NDOCS="${NDOCS:-10000}"
NTERMS="${NTERMS:-10000}"
NQ="${NQ:-2000}"
DATADIR="$(mktemp -d "${TMPDIR:-/tmp}/scdbench.XXXXXX")"

echo "Module : $MODULE"
echo "Docs   : $NDOCS   Dict terms: $NTERMS   Queries/run: $NQ   Port: $PORT"

redis-server --port "$PORT" --save '' --appendonly no --dir "$DATADIR" \
  --enable-debug-command yes --loadmodule "$MODULE" >/dev/null 2>&1 &
SRV=$!
trap 'kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; rm -rf "$DATADIR"' EXIT

for _ in $(seq 1 100); do
  [ "$(redis-cli -p "$PORT" ping 2>/dev/null)" = PONG ] && break; sleep 0.1
done

redis-cli -p "$PORT" FT.CONFIG SET DEFAULT_DIALECT 2 >/dev/null
redis-cli -p "$PORT" FT.CREATE idx ON HASH SCHEMA text TEXT >/dev/null

# Deterministic lowercase corpus (matches the micro-bench shape). Docs feed
# the index terms trie; the dictionaries are filled with distinct words from
# the same distribution so include-dict suggestions hit GetScore lookups.
python3 - "$NDOCS" "$NTERMS" <<'PY' | redis-cli -p "$PORT" --pipe
import sys, random
ndocs, nterms = int(sys.argv[1]), int(sys.argv[2])
random.seed(42)
alpha = "abcdefghijklmnopqrstuvwxyz"
def word(): return "".join(random.choice(alpha) for _ in range(random.randint(3, 12)))
def resp(*a):
    out = ["*%d\r\n" % len(a)]
    for x in a:
        x = str(x); out.append("$%d\r\n%s\r\n" % (len(x), x))
    return "".join(out)
w = sys.stdout.write
for i in range(ndocs):
    w(resp("HSET", "doc:%d" % i, "text", " ".join(word() for _ in range(6))))
seen = set()
while len(seen) < nterms:
    seen.add(word())
terms = sorted(seen)
CHUNK = 500
for i in range(0, len(terms), CHUNK):
    w(resp("FT.DICTADD", "dict_incl", *terms[i:i+CHUNK]))
    w(resp("FT.DICTADD", "dict_excl", *terms[i:i+CHUNK]))
PY

# Wait for indexing to drain.
for _ in $(seq 1 200); do
  ind="$(redis-cli -p "$PORT" FT.INFO idx | awk '/percent_indexed/{getline; print}')"
  [ "$ind" = "1" ] && break; sleep 0.1
done
echo "Indexed: num_docs=$(redis-cli -p "$PORT" FT.INFO idx | awk '/num_docs/{getline; print}')"
echo "Dict size: $(redis-cli -p "$PORT" FT.DICTDUMP dict_incl | wc -l | tr -d ' ')"
echo

# One fixed misspelled query word: a dict-distribution word with a digit
# substituted in, so it is distance 1 from real terms and matches nothing
# exactly. redis-benchmark repeats the identical command, which is fine —
# every FT.SPELLCHECK run re-walks the dictionaries.
QWORD="referen0e"

bench() { # label command...
  printf '%-34s ' "$1"; shift
  # redis-benchmark interleaves \r-progress with the final line even under -q;
  # keep only the last (summary) line.
  redis-benchmark -p "$PORT" -n "$NQ" -c 1 -q "$@" \
    | tr '\r' '\n' | grep 'requests per second' | tail -1 | sed 's/^[^:]*: //'
}

echo "=== command latency (single client) ==="
bench "SPELLCHECK (terms trie only)"   FT.SPELLCHECK idx "$QWORD"
bench "SPELLCHECK INCLUDE dict"        FT.SPELLCHECK idx "$QWORD" TERMS INCLUDE dict_incl
bench "SPELLCHECK INCLUDE, dist 2"     FT.SPELLCHECK idx "$QWORD" DISTANCE 2 TERMS INCLUDE dict_incl
bench "SPELLCHECK EXCLUDE dict"        FT.SPELLCHECK idx "$QWORD" TERMS EXCLUDE dict_excl
bench "DICTDUMP"                       FT.DICTDUMP dict_incl
bench "DICTADD dup (no-op add)"        FT.DICTADD dict_incl "$QWORD"x
echo
echo "=== DEBUG RELOAD (aux RDB save+load, 20 runs) ==="
redis-benchmark -p "$PORT" -n 20 -c 1 -q DEBUG RELOAD \
  | tr '\r' '\n' | grep 'requests per second' | tail -1 | sed 's/^[^:]*: /RELOAD: /'
