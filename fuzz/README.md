# RediSearch stateful fuzzer

Design and plan: [`openspec/changes/add-stateful-fuzzer/`](../openspec/changes/add-stateful-fuzzer/).

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
CMake-fetched sources, Redis), and `release-assets.githubusercontent.com`
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
3. Builds the module with `./build.sh SAN=address`, which is a Debug build, so
   assertions are enabled.
4. Writes `bin/fuzz/env.sh`, which exports `REDIS_SERVER`, `REDISEARCH_MODULE`,
   and the sanitizer settings below.

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
- Header generation (`cheadergen`) opens Cargo's SQLite cache and fails with
  `unable to open database file` where that is blocked. The committed headers
  are current on master, so pass `REDISEARCH_GENERATE_HEADERS=OFF` to
  `build.sh` unless the change touches FFI.

## Running

```bash
source bin/fuzz/env.sh
cd fuzz
cargo run -- run seeds/smoke.json --verbose
cargo run -- run <program.json> --repeat 10
```

`run` starts a fresh server, executes the program, and prints each finding.
`--repeat N` runs it N times and reports how often each finding reproduced.
Each distinct finding is written to `findings/<signature>/`: the program, the
trace, the server log, and a manifest with the binaries, sanitizer settings,
and reproduction rate. The exit code is 0 without findings, 1 with findings,
and 2 on an error such as a server that fails to start.

## Testing

```bash
cd fuzz
cargo test                                         # unit tests, no server needed
source ../bin/fuzz/env.sh && cargo test -- --ignored   # every program in tests/programs/ against a live server
```

The programs under `tests/programs/` exercise the harness itself: a crash, a
Redis panic, a hang, a stall the harness resolves, a barrier that cannot
complete, and a query left pending while another connection drops its index.
