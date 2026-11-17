# RediSearch stateful fuzzer

Design and plan: [`openspec/changes/add-stateful-fuzzer/`](../openspec/changes/add-stateful-fuzzer/).

The fuzzer is two crates in one Cargo workspace:

- [`procfuzz/`](procfuzz/) knows nothing about RediSearch or Redis. It owns
  the stateful program model, the interpreter, mutation and reduction, edge
  coverage through a small C shim, sanitizer reports, finding directories,
  and the LibAFL loop.
- [`redisearch/`](redisearch/) plugs RediSearch into it: its own ops (create,
  alter, query, populate), a generator, `redis-server` as a
  `procfuzz::server::Server`, and logic rules over search results. Its seeds,
  live-test programs, and planted bugs live next to it.

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
source bin/fuzz/env.sh
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
6. Writes `bin/fuzz/env.sh`, which exports `REDIS_SERVER`, `REDISEARCH_MODULE`,
   `REDISEARCH_PLANTED_MODULE`, `REJSON_MODULE`, and the sanitizer settings
   below.

Variables:

| Variable | Default | Effect |
|---|---|---|
| `CC` | `/opt/homebrew/opt/llvm/bin/clang` on macOS, `clang` on Linux | Compiler for both builds; the script refuses clang below 22. |
| `REDIS_SRC` | unset | Use this local Redis checkout instead of fetching from GitHub. |
| `FUZZ_OUT` | `bin/fuzz` | Output directory. |
| `JOBS` | `8` | Parallel jobs for the Redis build. |

### Runtime environment

`bin/fuzz/env.sh` sets:

| Variable | Value | Why |
|---|---|---|
| `ASAN_OPTIONS` | `halt_on_error=1:detect_odr_violation=0:detect_leaks=1` | The module is built with `-fsanitize-recover=all`; halting makes each run yield one report. |
| `LSAN_OPTIONS` | `suppressions=tests/memcheck/asan.supp` | Same suppressions as the Python tests. |
| `RS_GLOBAL_DTORS` | `1` | Without it the module skips its teardown and every global allocation reports as a leak. |
| `LD_PRELOAD` | `libstdc++.so.6` (Linux only) | Lets ASan intercept `__cxa_throw`, as in `tests/pytests/runtests.sh`. |

The server must be started with `--enable-debug-command yes` and no persistence
(`--save ""`, `--appendonly no`). In standalone mode the debug command is
`_FT.DEBUG`, not `FT.DEBUG`.

### Checking the setup

```bash
source bin/fuzz/env.sh
dir=$(mktemp -d)
"$REDIS_SERVER" --port 0 --unixsocket "$dir/s.sock" --save "" --appendonly no \
  --dir "$dir" --enable-debug-command yes --loadmodule "$REDISEARCH_MODULE" \
  > "$dir/log" 2>&1 &
until redis-cli -s "$dir/s.sock" PING > /dev/null 2>&1; do sleep 0.1; done
redis-cli -s "$dir/s.sock" FT.CREATE idx ON HASH SCHEMA t TEXT
redis-cli -s "$dir/s.sock" SHUTDOWN NOSAVE
wait
grep -E 'ERROR: (Address|Leak)Sanitizer' "$dir/log" || echo "clean"
```

A clean shutdown must report no sanitizer errors. Measured on an Apple Silicon
Mac: `redis-server` alone is ready in about 0.5 s; with the module, about 1.6 s.

### Known pitfalls

- **Stale sccache server.** `build.sh` uses sccache when one is installed. If a
  sandboxed process (for example a coding agent) starts the sccache server, the
  server inherits that sandbox and every later compile fails with
  `failed to write temporary file`. Run `sccache --stop-server`, then start it
  from an unrestricted shell.
- **Mixed toolchains.** A module built with one clang and a `redis-server`
  built with another can hang or abort at startup. Rebuild both with the same
  `CC`.

### Running inside a restricted sandbox

These apply only when the build runs inside a filesystem and network sandbox,
not in CI:

- `redis-server` must be allowed to listen on a local socket.
- CMake fetches several sources with `git clone`, which writes `.git/config`
  and `.git/hooks`. A sandbox that forbids those writes cannot run the first
  configure; run it once outside the sandbox.
- The Boost tarball contains a `.gitmodules` file. A sandbox that forbids
  writing it cannot extract Boost; point `.install/boost` at an existing copy.
- A new build directory, such as the first `FUZZ_COV` build, clones its CMake
  dependencies the same way. If another build directory already has them,
  pass `FETCHCONTENT_SOURCE_DIR_<NAME>=<that directory>/_deps/<name>-src` for
  each (`CPU_FEATURES`, `EVE`, `FMT`, `ROBINMAP`, `SPDLOG`, `TOMLPLUSPLUS`).
- Header generation (`cheadergen`) opens Cargo's SQLite cache and fails with
  `unable to open database file` where that is blocked. The committed headers
  are current on master, so pass `REDISEARCH_GENERATE_HEADERS=OFF` to
  `build.sh` unless the change touches FFI.

## Edge coverage

`FUZZ_COV=1` compiles C and C++ with `-fsanitize-coverage=trace-pc-guard` and
Rust with the equivalent `sancov-module` passes, so both languages number their
edges in one id space. It needs clang, and is rejected together with any option
that builds or runs tests, because test binaries do not link the shim.

For each server the fuzzer creates a POSIX shared-memory segment and names it
in `PROCFUZZ_COVERAGE_SHM`. When the module loads, the shim maps that segment and
records each edge it reaches by setting one byte. Because the bytes go straight
to shared memory, edges reached by a forked GC child or just before a crash are
kept. The fuzzer reads the map only after the whole process group has exited.
A module built without `FUZZ_COV` runs normally with an empty edge set.

## Running

```bash
source bin/fuzz/env.sh
cd fuzz/redisearch
RUST_LOG=debug cargo run -- run seeds/smoke.json
cargo run -- run <program.json> --repeat 10
cargo run -- fuzz --duration 3600
cargo run -- reduce <program.json>
cargo run -- generate --seed 7 > program.json
```

`run` starts a fresh server, executes the program, and logs each finding;
`RUST_LOG=debug` also logs every step. `--repeat N` runs it N times and
reports how often each finding reproduced and how many edges were reached in
every run versus in any run. Each distinct finding is written to
`findings/<signature>/`: the program, the trace, the server log, and a
manifest with the binaries, sanitizer settings, and reproduction rate. The exit code is 0 without findings, 1 with findings,
and 2 on an error such as a server that fails to start. `fuzz` runs a
coverage-guided campaign in one worker thread per core (`--jobs N` to
change), each from the programs in `seeds/` and its own corpus kept in
`corpus/<worker>/`, writing each finding whose signature is new to
`findings/`. A signature that already has a directory there is not reported
again, so creating an empty directory marks a known bug.

`reduce` shrinks a failing program while each smaller version still yields
the same finding (the first one, or the one named by `--signature`), then runs
the result ten times and bundles it like `run`, with the input as
`original.json`. `generate` prints a generated program; the same seed gives the
same program.

## Testing

```bash
cd fuzz
cargo test --workspace                                       # unit tests, no server needed
source ../bin/fuzz/env.sh && cargo test -- --ignored         # live tests against a real server
```

The live tests check that every seed runs without an error reply and reaches
code no other seed does; exercise the harness with the programs under
`tests/programs/` (a hang, a stall the harness releases, a barrier that cannot
complete, a query left pending while another connection drops its index);
check that most replies to generated programs are not errors, that a crash
inside a generated program reduces to the crashing command, that the programs
under `planted/` give a real sanitizer report and a real assertion, and that a
short campaign from the seeds finds a planted bug.
