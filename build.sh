#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build"

usage() {
  cat <<'USAGE'
Usage: ./build.sh <command>

Commands:
  clean              Remove build artifacts
  tests              Configure and build library and C++ tests (GoogleTest)
  run_unit_tests     Run the unit tests via ctest (add -v for verbose)

Notes:
- This builds RediSearch as a static library and links our disk code and tests against it.
- Tests are enabled via -DBUILD_DISK_TESTS=ON and run with CTest.
USAGE
}

cmd_clean() {
  echo "[clean] Removing ${BUILD_DIR}"
  rm -rf "${BUILD_DIR}"
}

cmd_tests() {
  local build_type="${CMAKE_BUILD_TYPE:-RelWithDebInfo}"
  echo "[configure] CMake configure (tests enabled, type=${build_type})"
  cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${build_type}" -DBUILD_DISK_TESTS=ON
  echo "[build] Building"
  cmake --build "${BUILD_DIR}" -j"$(nproc)"
}

cmd_run_unit_tests() {
  local verbose=0
  # Parse optional flags (currently only -v)
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -v|--verbose)
        verbose=1
        shift
        ;;
      *)
        echo "Unknown option for run_unit_tests: $1"; exit 2;
        ;;
    esac
  done

  if [[ ! -d "${BUILD_DIR}" ]]; then
    echo "Build directory missing. Running configure+build first..."
    cmd_tests
  fi
  if [[ $verbose -eq 1 ]]; then
    echo "[test] Running CTest (serialized, verbose)"
    ctest --test-dir "${BUILD_DIR}" --output-on-failure -j1 -V
  else
    echo "[test] Running CTest (serialized)"
    ctest --test-dir "${BUILD_DIR}" --output-on-failure -j1
  fi
}

main() {
  local cmd="${1:-}" || true
  case "${cmd}" in
    clean) shift; cmd_clean "$@" ;;
    tests) shift; cmd_tests "$@" ;;
    run_unit_tests) shift; cmd_run_unit_tests "$@" ;;
    -h|--help|help|"") usage ;;
    *) echo "Unknown command: ${cmd}"; usage; exit 2 ;;
  esac
}

main "$@"
