---
name: build
description: Build and validate RediSearch after code, dependency, bootstrap, toolchain, or build-system changes. Use before end-to-end tests and whenever build or test prerequisites change.
---

# Build Skill

Compile the project to verify changes build successfully.

## Usage
Run this skill after code, dependency, bootstrap, toolchain, or build-system changes.

## Instructions

### Local sccache Environment
Before running a build command locally, enable `sccache` for both C/C++ and Rust if it is
installed. This snippet assumes a Bash/POSIX shell, matching `build.sh`; on native Windows
PowerShell or `cmd.exe`, use Git Bash/WSL or set equivalent environment variables manually.
Run the setup in the same shell invocation as the build command so the exported variables apply.
```bash
if command -v sccache >/dev/null 2>&1; then
  repo_root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
  repo_parent="$(cd "$repo_root/.." && pwd)"
  export SCCACHE_CACHE_SIZE="${SCCACHE_CACHE_SIZE:-30G}"
  export SCCACHE_BASEDIRS="${SCCACHE_BASEDIRS:-$repo_parent}"
  if SCCACHE_PROBE_OUTPUT="$(sccache --show-stats 2>&1)"; then
    export RUSTC_WRAPPER="${RUSTC_WRAPPER:-sccache}"
  else
    echo "WARNING: sccache server failed to start; building Rust without sccache" >&2
    echo "$SCCACHE_PROBE_OUTPUT" >&2
    case "${RUSTC_WRAPPER-}" in
      sccache|*/sccache) unset RUSTC_WRAPPER ;;
    esac
  fi
fi
```
`build.sh` already configures CMake to use `sccache` for C/C++ when it is available.
`RUSTC_WRAPPER` is needed so Cargo also routes Rust compilation through `sccache`.
The default `SCCACHE_BASEDIRS` is the parent directory of the current Git checkout, so sibling
worktrees share normalized paths. Override it explicitly if worktrees live in multiple locations.
Use the OS-specific separator required by `sccache` when setting multiple base directories.

If verifying cache use, run:
```bash
sccache --show-stats
```
Check that `Base directories` and `Max cache size` reflect the exported settings. If they do
not, the `sccache` server was probably already running with older settings. Do not restart it
while another build may be active; otherwise restart it once after exporting the variables:
```bash
sccache --stop-server || true
sccache --start-server
```

Do not set `CARGO_INCREMENTAL=0` by default. Use it only when the goal is better Rust cache
reuse across clean worktrees, since disabling incremental compilation can slow tight local
edit-build loops in a single worktree.

### Bootstrap and Dependency Changes

Use this checklist when changing `.install/`, bootstrap Make targets, CI provisioning,
minimum tool versions, or a dependency used by builds, tests, coverage, sanitizers, or linting.
Run the commands from the RediSearch repository root.

Treat the bootstrap modes as one interface backed by the real installer:

| Command | Expected behavior |
|---|---|
| `make bootstrap list` | Report satisfied and missing prerequisites without changing the system |
| `make bootstrap dry-run` | Print only the actions needed for the current machine without changing it |
| `make bootstrap` | Install or upgrade the required prerequisites |
| `make build` | Verify the normal build after bootstrap |
| `make test` | Run all test families; prefer a narrower target when it exercises the changed dependency |

`make bootstrap` installs system packages and may use `sudo`; run it only when that mutation is
authorized. `list` and `dry-run` are the safe first checks.

For every new or changed dependency:

1. Wire detection, minimum-version handling, and installation through the real installer so
   `list`, `dry-run`, and bootstrap cannot drift into separate dependency definitions.
2. Run `make bootstrap list`, `make bootstrap dry-run`, and
   `make bootstrap dry-run | bash -n`. Dry-run output must be non-mutating, shell-valid, and
   usable as an installation plan.
3. Exercise missing, outdated, and already-sufficient states when version handling changes.
   Dry-run depends on the host's current state, so use a disposable container for states the
   development machine cannot represent. Execute generated dry-run commands only in such a
   disposable environment.
4. Run the real `make bootstrap`, confirm `make bootstrap list` is satisfied afterward, and run
   `make build` to verify the resulting environment.
5. Run the smallest test target that consumes the dependency: `make unit-tests`,
   `make rust-tests`, or `make pytest TEST=<test>`. Use `make test` when the dependency or
   bootstrap refactor can affect every test family; do not default to the full suite for a
   narrowly scoped installer change.

Repeat the relevant checks with feature flags that activate the dependency. For example, a
coverage dependency requires both the ordinary path above and:

```bash
make bootstrap list COV=1
make bootstrap dry-run COV=1
make bootstrap dry-run COV=1 | bash -n
make bootstrap COV=1
make build COV=1
```

The ordinary path must remain usable without invoking an opt-in tool unless the change
intentionally makes that tool a general prerequisite.

### Full Build (C + Rust)
```bash
./build.sh
```
Use this when you modified C code, or when building for the first time.

### Debug Build (recommended for development)
```bash
./build.sh DEBUG=1
```
Enables debug symbols and additional assertions. Use this when developing or debugging.

### Rust-Only Build (faster iteration)
```bash
cargo build --manifest-path src/redisearch_rs/Cargo.toml
```
Only use after the C code has been built at least **once** with `./build.sh`.
If you update C code, run `./build.sh` again before the Rust-only build.

### Build with Tests
```bash
./build.sh TESTS
```
Compiles test binaries (C/C++ unit tests) alongside the module. Required before
running individual test binaries directly (e.g., `rstest --gtest_filter=...`).

### If Build Fails

- Read the compiler errors carefully.
- C errors: check for missing includes, incompatible pointer types, implicit function
  declarations (all promoted to errors).
- Rust errors: check for FFI signature mismatches if C headers changed.
- Fix the issues and re-run the build.

## Clean Build

If you encounter strange build errors (stale artifacts, CMake cache issues):
```bash
./build.sh FORCE
```

For Rust only:
```bash
(cd src/redisearch_rs && cargo clean && cargo build)   # subshell keeps clean+build co-located
```
