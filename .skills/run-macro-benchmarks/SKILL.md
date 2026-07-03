---
name: run-macro-benchmarks
description: Run a RediSearch end-to-end macro benchmark from tests/benchmarks/*.yml via redisbench-admin. Use this when you want to measure whole-module performance (throughput/latency) against a real redis-server, not micro Rust or C++ benchmarks.
---

# Run Macro Benchmark Skill

Run a RediSearch end-to-end macro benchmark (a `tests/benchmarks/*.yml` config) against
a real `redis-server` using `redisbench-admin run-local`.

For Rust micro-benchmarks (Criterion), use `/run-rust-benchmarks` instead.

## Arguments
- No arguments: List the available benchmarks in `tests/benchmarks/` and ask which to run.
- `<benchmark>`: Run the given benchmark. Accepts the bare name
  (e.g. `search-expire-doc-json-10-milliseconds`), the file name with extension
  (e.g. `search-expire-doc-json-10-milliseconds.yml`), or the full path
  (e.g. `tests/benchmarks/search-expire-doc-json-10-milliseconds.yml`).

Arguments provided: `$ARGUMENTS`

## Prerequisites

Confirm these before running (do not re-install if already present):

1. **The modules are built.** There must be a *release* `redisearch.so` and a *release*
   `rejson.so` under `bin/`. Filter to the `*-release/*` build flavor — a workspace can also
   hold a `linux-x64-debug` (DEBUG=1) or coverage build, and benchmarking the wrong binary
   silently reports numbers for it:
   ```bash
   find "$(pwd)/bin" -name redisearch.so -path '*-release/*'
   find "$(pwd)/bin" -name rejson.so -path '*-release/*'
   ```
   Each must resolve to exactly one path. If either yields more than one candidate, stop and
   ask the user which build to benchmark rather than guessing (the run command below aborts
   on ambiguity). If `redisearch.so` is missing, build with `./build.sh` (or invoke `/build`). If
   `rejson.so` is missing, build it with `./tests/deps/setup_rejson.sh`. RedisJSON is always
   loaded (like CI) so JSON benchmarks work; without it their dataset load fails with
   `unknown command 'JSON.SET'` and the run aborts on a keyspace check.
2. **`redis-server`, `memtier_benchmark`, and `ftsb_redisearch` are on `$PATH`** — some
   benchmarks need `memtier_benchmark` and/or `ftsb_redisearch`. See `developer.md`.
3. **Python benchmark deps are installed** into the project virtualenv:
   ```bash
   uv pip install -r ./tests/benchmarks/requirements.txt
   ```
4. **Port 6379 is free.** `run-local` starts its own `redis-server` on 6379. If another
   `redis-server` is already listening there, redisbench-admin's server fails to bind
   (`Address already in use`) and aborts, but the benchmark then **silently loads into the
   stale server** — which usually lacks the right modules — and fails a keyspace check with
   `0 != <expected>` keys. Check first:
   ```bash
   ss -ltn | grep -q ':6379 ' && echo "PORT 6379 BUSY" || echo "port 6379 free"
   ```
   If busy, do NOT kill it yourself — it may be a workload the user cares about. Report it
   and ask the user to stop it (e.g. `redis-cli -p 6379 shutdown nosave`, or by typing
   `! redis-cli -p 6379 shutdown nosave` in the prompt).

## Instructions

1. Resolve the benchmark argument to a `tests/benchmarks/<benchmark>.yml` path. If no
   argument was given, list `tests/benchmarks/*.yml` and ask the user which to run.
2. Run the benchmark with he following commands. Capture output to a log per the
   "Running Expensive Commands" guidance in `CLAUDE.md`:
   ```bash
   set -o pipefail

   # Resolve exactly one *release* module; refuse to guess (a debug/coverage build
   # under bin/ would otherwise be benchmarked and mislabelled as release numbers).
   resolve_release_module() {
       local name=$1 matches n
       matches=$(find "$(pwd)/bin" -name "$name" -path '*-release/*')
       n=$(printf '%s' "$matches" | grep -c .)
       if [ "$n" -ne 1 ]; then
           echo "ERROR: expected exactly one release $name under bin/, found $n:" >&2
           printf '  %s\n' $matches >&2
           return 1
       fi
       printf '%s\n' "$matches"
   }
   REDISEARCH_SO=$(resolve_release_module redisearch.so) || exit 1
   REJSON_SO=$(resolve_release_module rejson.so) || exit 1

   LOG=$(mktemp /tmp/macrobench.XXXXXX.log)
   echo "Log: $LOG"
   uv run redisbench-admin run-local \
       --module_path "$REDISEARCH_SO" \
       --required-module search \
       --module_path "$REJSON_SO" \
       --required-module ReJSON \
       --allowed-setups oss-standalone \
       --allowed-envs oss-standalone \
       --test tests/benchmarks/<benchmark>.yml \
       2>&1 | tee "$LOG" | tail -40
   ```
3. **Run the benchmark only once.** If the output is truncated or you need a specific
   number, `grep`/`rg` the saved `$LOG` — do not re-run the benchmark to see more output.
4. Do NOT run the benchmark concurrently with any `./build.sh`, `make`, or `cargo`
   command — they contend on the shared Cargo build directory and running redis instances,
   and concurrency skews benchmark timings.
5. When the run completes, summarize the key results (throughput ops/sec, latency
   percentiles) from the output. If the user is comparing against another build, ask
   whether they want a copyable markdown table for a PR description.
