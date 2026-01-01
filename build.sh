#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build"

usage() {
  cat <<'USAGE'
Usage: ./build.sh <command> [options]

Commands:
  clean            Remove build artifacts
  build            Build artifacts
  lint             Lint Rust code (redisearch_disk)
  lint-vecsim      Lint C++ code (vecsim_disk)
  format-vecsim    Fix C++ code formatting (vecsim_disk)
  test             Run Rust unit tests (redisearch_disk)
  test-vecsim      Run C++ unit tests (vecsim_disk)
  test-miri        Run Rust unit tests with Miri (requires nightly toolchain)
  test-flow        Run integration tests with RLTest
  bench            Run Rust micro benchmarks
  profile          Profile the module with VTune

Environment Variables:
  PROFILE     Build profile: Debug (default) or Release
              Example: PROFILE=Release ./build.sh build

Rust Unit Tests (./build.sh test):
  Run all Rust tests:
    ./build.sh test

  Run a specific test:
    ./build.sh test <test_name>
    Example: ./build.sh test test_db_basic

  Run tests in a specific file:
    ./build.sh test --test <test_file>
    Example: ./build.sh test --test doc_table

  Run with verbose output:
    ./build.sh test -- --nocapture

  Additional cargo test options can be passed directly:
    ./build.sh test -- --nocapture --test-threads=1

Miri Tests (./build.sh test-miri):
  Run all tests with Miri (detects undefined behavior):
    ./build.sh test-miri

  Run a specific test with Miri:
    ./build.sh test-miri <test_name>
    Example: ./build.sh test-miri test_db_basic

  Customize Miri flags:
    MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-ignore-leaks" ./build.sh test-miri

  Note: Miri requires nightly toolchain and cannot test FFI code.
        Install with: rustup toolchain install nightly
                      rustup component add --toolchain nightly miri
        Default MIRIFLAGS: -Zmiri-disable-isolation -Zmiri-backtrace=full

Integration Tests (./build.sh test-flow):
  Run all integration tests:
    ./build.sh test-flow --redis-lib-path /usr/local/lib

  Run a specific test file:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --test test_basic.py

  Run a specific test function:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --test test_basic.py::test_module_loads_successfully

  Run a specific BDD scenario:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --test "features/basic.feature::Create a basic search index"

  Run with verbose output:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --verbose

  Run tests in parallel:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --parallel

  Use custom Redis server:
    ./build.sh test-flow --redis-lib-path /usr/local/lib --redis-server /path/to/redis-server

  Additional pytest options can be passed directly:
    ./build.sh test-flow --redis-lib-path /usr/local/lib -- -k "search" -v

Micro Benchmarks (./build.sh bench):
  Run all benchmarks:
    ./build.sh bench

  Run a specific benchmark:
    ./build.sh bench --bench document_id_key

  Run benchmarks and save as baseline:
    ./build.sh bench -- --save-baseline my-baseline

  Compare against a baseline:
    critcmp my-baseline current

  Run benchmarks in test mode (quick validation):
    ./build.sh bench -- --test

  Additional cargo bench options can be passed after --:
    ./build.sh bench -- --verbose

  Note: Benchmarks always run in release mode for accurate performance measurement.
        Install critcmp for comparing results: cargo install critcmp

Profiling (./build.sh profile):
  Profile the module with VTune:
    ./build.sh profile

  Specify VTune result directory:
    ./build.sh profile --result-dir /path/to/results

  Note: Requires Intel VTune to be installed and available in PATH.
        The module is built in Release mode for accurate profiling.
        Redis server is started with the module loaded and configured with SEARCH.CLUSTERSET.
        After profiling, use vtune-gui to view the results.

Notes:
- This builds RediSearch as a static library and links our disk code.
- Integration tests require redis-server to be installed and in PATH.
- Integration tests require --redis-lib-path pointing to directory with bs_speedb.so
  (typically the redis repository src directory).
USAGE
}

cmd_clean() {
  echo "[clean] Removing ${BUILD_DIR}"
  rm -rf "${BUILD_DIR}"
}

cmd_lint() {
  echo "[lint] Linting Rust code (redisearch_disk)"
  cd "${ROOT_DIR}/redisearch_disk/"
  cargo clippy --color=always -- -D warnings
  cargo fmt -- --color=always --check
}

cmd_lint_vecsim() {
  echo "[lint-vecsim] Linting C++ code (vecsim_disk)"
  _run_clang_format check
}

cmd_format_vecsim() {
  echo "[format-vecsim] Fixing C++ code formatting (vecsim_disk)"
  _run_clang_format fix
}

_run_clang_format() {
  local mode="$1"  # "check" or "fix"
  local vecsim_dir="${ROOT_DIR}/vecsim_disk"

  # Find all C++ source files (excluding build directory)
  local sources=()
  while IFS= read -r -d '' file; do
    sources+=("$file")
  done < <(find "${vecsim_dir}" -path "${vecsim_dir}/build" -prune -o -type f \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \) -print0)

  if [ ${#sources[@]} -eq 0 ]; then
    echo "No C++ source files found"
    return 0
  fi

  if [ "$mode" = "check" ]; then
    local failed=false
    for file in "${sources[@]}"; do
      if ! clang-format --dry-run -Werror "$file" 2>&1; then
        failed=true
      fi
    done

    if [ "$failed" = true ]; then
      echo "Formatting check failed. Run './build.sh format' to fix."
      exit 1
    fi
    echo "Formatting check passed"
  else
    for file in "${sources[@]}"; do
      clang-format -i "$file"
    done
    echo "Formatting applied to ${#sources[@]} files"
  fi
}

cmd_build() {
  local profile="${PROFILE:-Debug}"
  echo "[configure] CMake configure (profile=${profile})"
  cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${profile}" ${CMAKE_ARGS:-}
  echo "[build] Building"
  cmake --build "${BUILD_DIR}" -j"$(nproc)"
}

cmd_test() {
  local profile="${PROFILE:-Debug}"
  # Convert CMake build type to Rust profile name
  local rust_profile="dev"
  if [ "${profile}" != "Debug" ]; then
    rust_profile="release"
  fi

  echo "[test] Running Rust unit tests (redisearch_disk)"
  cd "${ROOT_DIR}/redisearch_disk/"
  # Enable unittest feature to link C static libraries needed by tests
  cargo test --profile="${rust_profile}" --features unittest --color=always "$@"
}

cmd_test_vecsim() {
  local profile="${PROFILE:-Debug}"
  echo "[test-vecsim] Running C++ unit tests (vecsim_disk)"

  # Build vecsim_disk and test executable
  cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${profile}" ${CMAKE_ARGS:-}
  cmake --build "${BUILD_DIR}" --target vecsim_disk -j"$(nproc)"
  cmake --build "${BUILD_DIR}" --target test_hnsw_disk -j"$(nproc)" 2>/dev/null || true

  # Run CTest
  cd "${BUILD_DIR}/vecsim_disk"
  ctest --output-on-failure
}

cmd_test_miri() {
  echo "[test-miri] Running Rust tests with Miri"
  cd "${ROOT_DIR}/redisearch_disk/"
  # Set default MIRIFLAGS if not already set
  # -Zmiri-disable-isolation allows tests to access the file system and environment
  # -Zmiri-backtrace=full provides detailed backtraces for debugging
  export MIRIFLAGS="${MIRIFLAGS:--Zmiri-disable-isolation -Zmiri-backtrace=full}"
  cargo +nightly miri test "$@"
}

cmd_test_flow() {
  echo "[test-flow] Running integration tests with RLTest"
  cd "${ROOT_DIR}/flow-tests"
  python3 runtests.py --module "${BUILD_DIR}/redisearch.so" "$@"
}

cmd_bench() {
  echo "[bench] Running micro benchmarks"
  cd "${ROOT_DIR}/redisearch_disk/"
  # Benchmarks always run in release mode for accurate performance measurement
  # Use --benches to only run benchmark binaries, not library unit tests
  cargo bench --benches --color=always "$@"
}

cmd_profile() {
  # Build in Release mode
  echo "[profile] Building module in Release mode..."
  PROFILE=Release cmd_build

  # Source and run the profile script
  source "${ROOT_DIR}/profile.sh"
  run_profile "${ROOT_DIR}" "${BUILD_DIR}" "$@"
}

main() {
  local cmd="${1:-}" || true
  case "${cmd}" in
    clean) shift; cmd_clean "$@" ;;
    build) shift; cmd_build "$@" ;;
    lint)  shift; cmd_lint "$@" ;;
    lint-vecsim) shift; cmd_lint_vecsim "$@" ;;
    format-vecsim) shift; cmd_format_vecsim "$@" ;;
    test)  shift; cmd_test "$@" ;;
    test-vecsim) shift; cmd_test_vecsim "$@" ;;
    test-miri) shift; cmd_test_miri "$@" ;;
    test-flow) shift; cmd_test_flow "$@" ;;
    bench) shift; cmd_bench "$@" ;;
    profile) shift; cmd_profile "$@" ;;
    -h|--help|help|"") usage ;;
    *) echo "Unknown command: ${cmd}"; usage; exit 2 ;;
  esac
}

main "$@"
