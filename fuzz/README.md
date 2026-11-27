# RediSearch stateful fuzzer

Design and plan: [`openspec/changes/add-stateful-fuzzer/`](../openspec/changes/add-stateful-fuzzer/).

The fuzzer is three crates in one Cargo workspace, from generic to specific:

- [`procfuzz/`](procfuzz/) knows nothing about RediSearch, Redis, or RESP. A
  target writes one `Model`; `procfuzz` runs, mutates, and shrinks its
  programs, measures edge coverage through a small C shim, reads sanitizer
  reports, keeps finding directories, and runs the search loop.
- [`procfuzz-redis/`](procfuzz-redis/) holds what any Redis module target
  shares: the RESP2 codec, launching `redis-server` with modules, and reading
  Redis crash reports.
- [`redisearch/`](redisearch/) is RediSearch as a model: its ops, what they set
  up, which make sense when, what each command does, and logic
  rules over search results. Its seeds, live-test programs, and planted bugs
  live next to it.

## Setup

The fuzzer runs programs against an AddressSanitizer build of the module loaded
into an AddressSanitizer build of `redis-server`. Both must be built with the
same clang.

### Prerequisites

| Tool | Version | Notes |
|---|---|---|
| clang | 22 or newer | macOS: `brew install llvm` (keg-only at `/opt/homebrew/opt/llvm`). Homebrew `llvm@21`'s ASan runtime hangs at process start on macOS 26. Linux: the clang CI uses; gcc cannot build the sanitizer `redis-server` the module needs. |
| Rust | `rust-toolchain.toml` and the nightly named in `.rust-nightly` | `build.sh` switches to the nightly for `SAN=address`. The nightly needs the `rust-src` component (`-Zbuild-std`). |
| cmake, make, perl, git | as for a normal RediSearch build | |

Network access during the first build: crates.io, github.com (submodules,
CMake-fetched sources, Redis, RedisJSON), and `release-assets.githubusercontent.com`
(Boost, unless `.install/boost` already exists).

### One command

```bash
fuzz/setup.sh
```

`fuzz/setup.sh` is idempotent. It:

1. Initialises the git submodules under `deps/`.
2. Builds `redis-server` with `SANITIZER=address MALLOC=libc` at the Redis
   commit pinned in the script, into `bin/fuzz/redis-<sha>-asan/`. The pin
   matches the default in `.github/workflows/task-build-redis.yml`; update both
   together.
3. Builds the module with `./build.sh FUZZ_COV=1` into
   `bin/<os>-<arch>-debug-asan-fuzz/`. `FUZZ_COV` implies `SAN=address`, which
   is a Debug build, so assertions are enabled. It also instruments C, C++ and
   Rust for edge coverage and links `fuzz/procfuzz/shim/coverage.c` into the module; see
   [Edge coverage](#edge-coverage). Arguments to `setup.sh` are passed on to
   `build.sh`.
4. Builds the planted module, `bin/fuzz/planted/redisearch.so`: the same build
   with `fuzz/redisearch/planted/planted.patch` applied, which adds two known bugs for
   testing the fuzzer itself. The patch is applied to the working tree for the
   duration of one incremental build and reverted afterwards, then the module
   is rebuilt clean; do not commit or snapshot while it runs. If the patch no
   longer applies after a change on master, refresh its context lines.
5. Builds RedisJSON with `tests/deps/setup_rejson.sh`, exactly as
   `tests/pytests/runtests.sh` does: it clones RedisJSON into
   `tests/deps/RedisJSON` and builds it with `SAN=address` (on Linux; RedisJSON's
   sanitizer build does not support macOS, so there it is built without one). `REJSON_PATH`
   names a prebuilt module instead, and `REJSON_BRANCH` a branch to build
   (default `master`). Every fuzzed server loads it after the module.
6. Links the binaries as `bin/fuzz/redis-server`, `bin/fuzz/redisearch.so`,
   and `bin/fuzz/rejson.so`, and the `llvm-symbolizer` next to `CC` as
   `bin/fuzz/llvm-symbolizer`, where the fuzzer looks for them.

Variables:

| Variable | Default | Effect |
|---|---|---|
| `CC` | `/opt/homebrew/opt/llvm/bin/clang` on macOS, `clang` on Linux | Compiler for both builds; the script refuses clang below 22. |
| `REDIS_SRC` | unset | Use this local Redis checkout instead of fetching from GitHub. |
| `JOBS` | `8` | Parallel jobs for the Redis build. |

### Server environment

The fuzzer starts every server with:

| Variable | Value | Why |
|---|---|---|
| `ASAN_OPTIONS` | `halt_on_error=1:detect_odr_violation=0:detect_leaks=1:external_symbolizer_path=…/llvm-symbolizer` | The module is built with `-fsanitize-recover=all`; halting makes each run yield one report. macOS's default symbolizer, `atos`, takes seconds per run. |
| `LSAN_OPTIONS` | `suppressions=tests/memcheck/asan.supp` | Same suppressions as the Python tests. |
| `RS_GLOBAL_DTORS` | `1` | Without it the module skips its teardown and every global allocation reports as a leak. |

and with `--enable-debug-command yes` and no persistence (`--save ""`,
`--appendonly no`). In standalone mode the debug command is `_FT.DEBUG`, not
`FT.DEBUG`. On Linux, also export `LD_PRELOAD=$(g++
-print-file-name=libstdc++.so.6)`, as `tests/pytests/runtests.sh` does, so
that ASan intercepts `__cxa_throw`.

### Checking the setup

```bash
cd fuzz/redisearch
cargo test -- --ignored every_seed
```

It runs every seed and must pass. Measured on an Apple Silicon Mac, a server
with the module starts in about 0.1 s and stops in about 0.4 s, most of it
LeakSanitizer's check at exit.

### Known pitfalls

- **Stale sccache server.** `build.sh` uses sccache when one is installed. If a
  sandboxed process (for example a coding agent) starts the sccache server, the
  server inherits that sandbox and every later compile fails with
  `failed to write temporary file`. Run `sccache --stop-server`, then start it
  from an unrestricted shell.
- **Mixed toolchains.** A module built with one clang and a `redis-server`
  built with another can hang or abort at startup. Rebuild both with the same
  `CC`.

### Running

```bash
cd fuzz/redisearch
cargo run --release -- fuzz --seeds seeds --duration 3600
cargo run --release -- reduce <program.json>
cargo run -- generate --seed 7 --ops 30 > program.json
cargo run -- import commands.txt > program.json
```

The binaries are the ones under `bin/fuzz/`, or those named by
`--redis-server`, `--module`, and `--rejson`. `--module
../../bin/fuzz/planted/redisearch.so` fuzzes the module with planted bugs.

`fuzz` runs a coverage-guided campaign in one worker thread per core
(`--jobs N` to change), logging a status line every 10 seconds. The workers
share the programs they keep, start from those in `--seeds` and in `corpus/`,
and write what they keep to `corpus/`. A program counts as new only if a
second run shows the same new features. Ctrl-C lets the runs in progress
finish and cleans up their servers.

Each new finding is reduced to the smallest program that still shows it, run
five more times for its reproduction rate, and written to
`findings/<finding>/`:

- `repro.py`, the finding to share: the reduced program as a standalone
  redis-py script against a server you start, saying how to start it and what
  goes wrong. A broken logic rule ends in the assertion that fails. To make a
  regression test, move its commands into a test with `getConnectionByEnv(env)`;
- `program.json` and `original.json`, the reduced and the found program;
- `trace.json`, `server.log`, and `manifest.json`, with the reproduction rate,
  the binaries and the files they resolve to, the server environment, and the
  timing.

A finding that already has a directory there is not reported again, so
creating an empty directory marks a known bug. `corpus/` and `findings/` are
ignored by git.

`reduce` does the same for a program at hand, for its first finding or the
one named by `--finding`; it exits with 1 when it wrote one, and 2 on an
error such as a server that fails to start. `generate` prints a generated
program; the same seed gives the same program. `import` reads `redis-cli`
lines, or a `MONITOR` recording, into a program on one connection.

To turn a Python test into a seed, run it against a server you started while
`redis-cli MONITOR` records:

```bash
redis-cli -p 6399 MONITOR > test.monitor &
(cd ../../tests/pytests && python -m RLTest --env existing-env \
  --existing-env-addr 127.0.0.1:6399 -t test_synonyms:testSynonymUpdate)
kill %1
grep -v -E '\] "(CLIENT|PING|INFO|FLUSHALL)"' test.monitor > test.txt
cargo run -- import test.txt > seeds/synonyms.json
```

Edit out commands that reply with an error, which the seed test rejects.

## Testing

```bash
cd fuzz
cargo test --workspace                                       # unit tests, no server needed
cargo test -- --ignored                                      # live tests, after fuzz/setup.sh
```

The live tests run against the real server: every seed runs without an
error reply and reaches code no other seed does; the programs under
`tests/programs/` give a hang, a stall that healing releases, and a settle
that cannot complete; most replies to generated programs are not errors; a
crash inside a generated program reduces to the crashing command; the
programs under `planted/` give a real sanitizer report and a real assertion;
a failing worker stops the campaign; and a short campaign from the seeds
finds a planted bug.
