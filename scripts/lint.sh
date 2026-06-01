#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#
# Unified lint script. Runs all linting checks, continues past failures,
# and prints a summary report at the end. Exit code is non-zero if any
# check failed.
#
# Usage:
#   scripts/lint.sh [options]
#
# Options (via environment variables):
#   CHECK=1                          Check formatting without modifying files (default in CI)
#   COMPARE_ADVISORIES_TO_BASE=true  Only fail advisories on new findings vs base ref
#   ADVISORIES_BASE_REF=<ref>        Git ref for advisory comparison
#
# CI sets these env vars; local runs use defaults (CHECK=0, no advisory comparison).

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUST_DIR="$ROOT/src/redisearch_rs"
MANIFEST_PATH="src/redisearch_rs/Cargo.toml"

# Extract exclude crates from build.sh
EXCLUDE_CRATES=$(grep "EXCLUDE_RUST_BENCHING_CRATES_LINKING_C=" "$ROOT/build.sh" | cut -d'=' -f2 | tr -d '"' | head -n1)

COMPARE_ADVISORIES_TO_BASE="${COMPARE_ADVISORIES_TO_BASE:-false}"
ADVISORIES_BASE_REF="${ADVISORIES_BASE_REF:-}"

cd "$ROOT"

# Track results: parallel arrays of check names and outcomes
declare -a CHECK_NAMES=()
declare -a CHECK_RESULTS=()

run_check() {
  local name="$1"
  shift
  CHECK_NAMES+=("$name")
  echo ""
  echo "==== $name ===="
  if "$@"; then
    CHECK_RESULTS+=("pass")
    echo "---- $name: PASSED ----"
  else
    CHECK_RESULTS+=("FAIL")
    echo "---- $name: FAILED ----"
  fi
}

# --- Header generation ---
run_check "Generate Rust headers" "$ROOT/src/redisearch_rs/regen_headers.sh"

# --- Clippy (debug) ---
run_check "Clippy (debug)" bash -c '
  cd "'"$RUST_DIR"'" && cargo clippy --workspace '"$EXCLUDE_CRATES"' -- -D warnings
'

# --- Cargo doc (debug) ---
run_check "Cargo doc (debug)" bash -c '
  cd "'"$RUST_DIR"'" && RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace '"$EXCLUDE_CRATES"' --no-deps --document-private-items
'

# --- Clippy (release) ---
run_check "Clippy (release)" bash -c '
  cd "'"$RUST_DIR"'" && cargo clippy --workspace '"$EXCLUDE_CRATES"' --release -- -D warnings
'

# --- Cargo doc (release) ---
run_check "Cargo doc (release)" bash -c '
  cd "'"$RUST_DIR"'" && RUSTDOCFLAGS="-Dwarnings" cargo doc --workspace '"$EXCLUDE_CRATES"' --no-deps --document-private-items --release
'

# --- License headers ---
run_check "License headers" bash -c 'cd "'"$RUST_DIR"'" && cargo license-check'

# --- Format check ---
run_check "Format (check)" bash -c '
  cd "'"$RUST_DIR"'" && cargo fmt --check --all
'

# --- Generated files (git diff) ---
run_check "Generated files (git diff)" bash -c '
  if ! git diff --exit-code; then
    echo "Uncommitted changes found. Likely causes: stale Rust → C FFI headers"
    echo "under src/redisearch_rs/headers/ (run '\''make generate-rust-headers'\'' and commit)"
    echo "or other auto-generated artifacts produced by lint."
    exit 1
  fi
'

# --- Security advisories ---
run_check "Security advisories" bash -c '
  ARGS=()
  if [ "'"$COMPARE_ADVISORIES_TO_BASE"'" = "true" ]; then
    ARGS+=(--compare-to-base --base-ref "'"$ADVISORIES_BASE_REF"'")
  fi
  python3 scripts/cargo_deny_advisory_gate.py \
    --manifest-path "'"$MANIFEST_PATH"'" \
    "${ARGS[@]}"
'

# --- Dependency licenses ---
run_check "Dependency licenses" bash -c '
  cd "'"$RUST_DIR"'" && cargo deny --all-features check licenses
'

# --- Workspace hack (hakari) ---
run_check "Workspace hack (hakari)" bash -c '
  cd "'"$RUST_DIR"'"
  if ! cargo hakari generate --diff || ! cargo hakari manage-deps --dry-run; then
    echo "Suggested fix:"
    echo "  Run the following in '\''src/redisearch_rs'\'':"
    echo "    cargo hakari generate && cargo hakari manage-deps"
    exit 1
  fi
'

# --- Summary report ---
echo ""
echo "========================================"
echo "  Lint Summary"
echo "========================================"

failures=0
for i in "${!CHECK_NAMES[@]}"; do
  name="${CHECK_NAMES[$i]}"
  result="${CHECK_RESULTS[$i]}"
  if [ "$result" = "FAIL" ]; then
    printf "  %-35s %s\n" "$name" "FAILED"
    failures=$((failures + 1))
  else
    printf "  %-35s %s\n" "$name" "ok"
  fi
done

echo "========================================"
total=${#CHECK_NAMES[@]}
passed=$((total - failures))
echo "  $passed/$total checks passed"
if [ "$failures" -gt 0 ]; then
  echo "  $failures FAILED"
fi
echo "========================================"

exit $((failures > 0 ? 1 : 0))
