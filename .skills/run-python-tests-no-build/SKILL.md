---
name: run-python-tests-no-build
description: Run Python end-to-end tests without rebuilding the module. Use this when iterating on Python test files only (no changes to C or Rust source) — invokes runtests.sh directly against the existing redisearch.so and rejson.so, skipping the ~90s build phase of ./build.sh RUN_PYTEST.
---

# Run Python Tests (No Build) Skill

Run Python tests directly via `tests/pytests/runtests.sh`, bypassing the CMake/Make/RedisJSON setup that `./build.sh RUN_PYTEST` performs on every invocation.

## When to use

- You are only editing files under `tests/pytests/`.
- The RediSearch module (`redisearch.so`) and the RedisJSON module (`rejson.so`) have already been built at least once by `./build.sh` (or `./build.sh DEBUG=1`).
- You want fast feedback loops (typical wall time: 1–10s per file instead of 1–2 minutes).

If you have modified any C, Rust, or other non-test source file since the last build, **rebuild first** with `./build.sh` and use the standard `/run-python-tests` skill — direct invocation will silently run the previous binary.

## Prerequisites

Pick the most recently built `.so` files and verify the module is fresh (no source file under `src/` is newer than it). Paste this block at the top of every shell that uses the skill — it both selects the binary and fails fast if a rebuild is required:

```bash
set -e
MODULE=$(realpath "$(ls -t bin/*/search-community/redisearch.so 2>/dev/null | head -1)")
REJSON_PATH=$(realpath bin/RedisJSON/master/rejson.so)
[ -f "$MODULE" ] && [ -f "$REJSON_PATH" ] || { echo "Module or RedisJSON .so not found under bin/ — run ./build.sh first."; exit 1; }

# Freshness check: bail out if any C/C++/Rust source is newer than the picked module.
STALE=$(find src -type f \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.hpp' -o -name '*.rs' \) -newer "$MODULE" -print -quit)
[ -z "$STALE" ] || { echo "Module is stale (e.g. $STALE is newer than $MODULE) — run ./build.sh and retry."; exit 1; }

export MODULE REJSON_PATH
echo "Using MODULE=$MODULE"
```

If you need a specific flavour (release vs debug) rather than "most recent", set `MODULE` by hand to e.g. `$(realpath bin/linux-x64-debug/search-community/redisearch.so)` before the freshness check.

## Instructions

### Single test

```bash
MODULE=$MODULE REJSON_PATH=$REJSON_PATH \
  TEST=test_max_timeout_limit:TestMaxForegroundTimeoutLimit.test_no_cap_when_within_limit \
  TEST_TIMEOUT=20 SA=1 REDIS_STANDALONE=1 \
  bash tests/pytests/runtests.sh
```

### Whole file (standalone)

```bash
MODULE=$MODULE REJSON_PATH=$REJSON_PATH \
  TEST=test_max_timeout_limit TEST_TIMEOUT=20 \
  SA=1 REDIS_STANDALONE=1 \
  bash tests/pytests/runtests.sh
```

### Whole file (cluster / coordinator)

```bash
MODULE=$MODULE REJSON_PATH=$REJSON_PATH \
  TEST=test_max_timeout_limit TEST_TIMEOUT=30 \
  SA=0 REDIS_STANDALONE=0 \
  bash tests/pytests/runtests.sh
```

### Capturing output

For longer runs, capture to a log per the AGENTS.md convention so you can inspect failures without rerunning:

```bash
set -o pipefail
LOG=$(mktemp /tmp/pytest-nobuild.XXXXXX.log)
echo "Log: $LOG"
MODULE=$MODULE REJSON_PATH=$REJSON_PATH \
  TEST=<file>[:<test>] TEST_TIMEOUT=20 SA=1 REDIS_STANDALONE=1 \
  bash tests/pytests/runtests.sh 2>&1 | tee "$LOG" | tail -40
```

## Important environment variables

| Variable | Purpose | Recommended value |
|---|---|---|
| `MODULE` | Absolute path to `redisearch.so` | `$(realpath bin/.../redisearch.so)` |
| `REJSON_PATH` | Absolute path to `rejson.so` (skips RedisJSON rebuild) | `$(realpath bin/RedisJSON/master/rejson.so)` |
| `TEST` | File or `file:Class.test` selector (no `.py` extension) | varies |
| `TEST_TIMEOUT` | Per-test timeout in seconds | `20` for quick feedback |
| `SA` / `REDIS_STANDALONE` | `1` = standalone, `0` = OSS cluster | per test target |
| `PARALLEL` | RLTest parallelism | `1` (lets it choose `nproc`) |
| `LOG_LEVEL` | Redis log level | `debug` |

Do **not** set `EXT` (defaults are correct). Do **not** set `BINROOT`/`FULL_VARIANT` unless you also set `REJSON_PATH`, because they trigger `setup_rejson.sh` to rebuild RedisJSON.

## Troubleshooting

- **`Error 111 connecting to localhost:6379. Connection refused.`**: Redis failed to start because the module rejected its arguments. Inspect `tests/pytests/logs/*master*.log` for a `# Module ... initialization failed` line. This almost always means the `.so` is stale relative to the test — rebuild with `./build.sh` and retry.
- **`No such configuration option \`...\``**: Same root cause as above (stale module).
- **`Module not found at <path>. Aborting.`**: `MODULE` points to a path that does not exist; double-check the architecture/flavor directory under `bin/`.
- **RedisJSON rebuilds despite `REJSON_PATH`**: The variable must be exported into the same shell that runs `runtests.sh`. Inline `MODULE=... REJSON_PATH=... bash runtests.sh` works; `export REJSON_PATH=...` in a parent shell also works.
- **First test in a class fails with connection refused while the others would have passed**: same stale-module symptom — the failure is in `__init__`, so the rest of the class is skipped.

## Output

Same format as the standard `/run-python-tests` skill — RLTest prints `[PASS]` / `[FAIL]` per test and a summary. Failed-test logs are under `tests/pytests/logs/`.
