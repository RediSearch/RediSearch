# MOD-16899 — JSONPath compile-cache bench (initial numbers)

Proof-of-concept measurements for caching the compiled JSONPath across document loads.

**Problem:** the JSON `LOAD` path calls RedisJSON `get(json, path_string)` once per document, which
re-runs the pest JSONPath parser every time. The parse result depends only on the path, not the
document, so it is pure redundant work — dominant CPU in JSON `LOAD`-heavy aggregates.

**Fix (PoC):** RedisJSON gains a V9 LLAPI `getWithPath(json, compiledPath)` that evaluates an
already-compiled path (from the existing `pathParse`). RediSearch compiles each `LOAD` field's path
once per query and reuses the handle, falling back to the string `get` when the API is < V9.

- RediSearch branch: `itzik-mod-16899-jsonpath-compile-cache`
- RedisJSON branch (fork): `Itzikvaknin/RedisJSON@itzik-mod-16899-getwithpath-llapi`
- Jira: MOD-16899

## Results

oss-standalone · single client · 100K skewed entity-events JSON docs · **before** = RedisJSON V8
(string `get`, recompiles per doc) · **after** = V9 `getWithPath` (compile once, reuse). Same
redisearch binary; only the JSON module differs. Exec latency = median of 250 runs; loader = median
of 50 `FT.PROFILE` Threadsafe-Loader samples (mean/median drift < 2.5%, so median is used).

| query | docs loaded | exec before→after | loader before→after |
|---|---|---|---|
| GROUPBY + COLLECT (the ticket) | 100,000 | 1823 → 517 ms (3.5×) | 1744 → 453 ms (3.8×) |
| Deep offset (no GROUPBY) | 50,099 | 336 → 119 ms (2.8×) | 314 → 99 ms (3.2×) |
| SORTBY + large page | 10,000 | 96 → 51 ms (1.9×) | 64 → 19 ms (3.3×) |

The win is not GROUPBY-specific — the two non-GROUPBY queries improve too. SORTBY's smaller
end-to-end (1.9×) vs loader (3.3×) is the 10K-row reply serialization, which the fix doesn't touch.

Open `mod16899_report.html` in a browser for the visual report.

## Files

| file | purpose |
|---|---|
| `mod16899_report.html` | the report (self-contained HTML) |
| `make_report.py` | generates the HTML from `results_{before,after}.json` |
| `measure2.py` | measurement harness: N exec runs + M `FT.PROFILE` runs → median/mean/min |
| `repro.py` | loads the synthetic dataset and defines the queries |
| `results2_{before,after}.json` | full stats (median, mean, min, n) at 250 exec / 50 profile |
| `results_{before,after}.json` | median-only inputs consumed by `make_report.py` |

## Reproduce

```bash
# 1. Build RediSearch (branch itzik-mod-16899-jsonpath-compile-cache)
./build.sh

# 2. Build both RedisJSON modules from tests/deps/RedisJSON:
#    - V8 (baseline): checkout master,                     cargo build --release -p redis_json
#    - V9 (fix):      checkout itzik-mod-16899-getwithpath-llapi, cargo build --release -p redis_json
#    keep both target/release/librejson.dylib copies (rejson_v8 / rejson_v9)

# 3. For each module (before=V8, after=V9): start redis with redisearch.so + that rejson, then
redis-server --port 6400 --save '' \
  --loadmodule <redisearch.so> --loadmodule <rejson_v{8,9}.dylib> &
python3 repro.py load  --port 6400 --docs 100000
redis-cli -p 6400 CONFIG SET search-enable-unstable-features yes   # for COLLECT
python3 measure2.py 6400 250 50 > results2_<before|after>.json

# 4. Convert medians into results_{before,after}.json and render:
python3 make_report.py
```
