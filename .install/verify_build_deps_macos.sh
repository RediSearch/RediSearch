#!/usr/bin/env bash

# Set colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
REQUIRED_CHEADERGEN_VERSION=$(cat "$REPO_ROOT/.cheadergen-version")

# Normally sourced by verify_build_deps.sh (which sources these first); source
# them defensively so this script also works if invoked on its own. All are
# idempotent.
source "$SCRIPT_DIR/version_compare.sh"
source "$SCRIPT_DIR/LLVM_VERSION.sh"
PINNED_RUST_VERSION=$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
  "$REPO_ROOT/rust-toolchain.toml" | head -1)

# The report state and helpers (record_ok / record_missing / emit_result /
# emit_deps_report / is_optional_dep / report_mode / missing_deps) come from
# verify_build_deps.sh. Provide minimal fallbacks so standalone use still works.
: "${missing_deps:=false}"
if ! declare -F emit_result >/dev/null 2>&1; then
  DEPS_OK="" DEPS_MISSING="" DEPS_OPT_OK="" DEPS_OPT_MISSING=""
  report_mode() { [ -n "${DEPS_REPORT_FILE:-}" ]; }
  is_optional_dep() { return 1; }
  record_ok() { DEPS_OK="$DEPS_OK $1"; }
  record_missing() { local t=$1; [ -n "${2:-}" ] && t="$1:$2"; DEPS_MISSING="$DEPS_MISSING $t"; missing_deps=true; }
  emit_result() {
    local dep=$1 status=$2 msg=${3:-} vtag=${4:-}
    if ! report_mode; then
      printf "%-20s" "$dep"
      [ "$status" = ok ] && echo -e "${GREEN}✓${NC}" || echo -e "${msg:-${RED}✗${NC}}"
    fi
    [ "$status" = ok ] && record_ok "$dep" || record_missing "$dep" "$vtag"
  }
  emit_deps_report() {
    report_mode || return 1
    local p
    for p in $DEPS_OK;         do echo "ok $p"          >> "$DEPS_REPORT_FILE"; done
    for p in $DEPS_MISSING;    do echo "missing $p"     >> "$DEPS_REPORT_FILE"; done
    for p in $DEPS_OPT_OK;     do echo "opt_ok $p"      >> "$DEPS_REPORT_FILE"; done
    for p in $DEPS_OPT_MISSING;do echo "opt_missing $p" >> "$DEPS_REPORT_FILE"; done
    return 0
  }
fi

should_check_cheadergen() {
  case "${REDISEARCH_GENERATE_HEADERS:-1}" in
    0|OFF|off|false|FALSE|False|NO|no)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

# ============================================
# Checkers
# ============================================

# Present-on-PATH check.
check_cmd_dep() {
  if command -v "$1" &>/dev/null; then
    emit_result "$1" ok
  else
    emit_result "$1" missing
  fi
}

# clang must be the LLVM build (Homebrew llvm@N), pinned to LLVM_VERSION — the
# same major Rust uses. Apple's system clang has a different versioning scheme
# and won't report LLVM_VERSION, so it's correctly rejected.
check_clang() {
  local tool=""
  if command -v "clang-${LLVM_VERSION}" &>/dev/null; then
    tool="clang-${LLVM_VERSION}"
  elif command -v clang &>/dev/null; then
    tool="clang"
  fi

  if [[ -z "$tool" ]]; then
    emit_result clang missing "${RED}✗${NC}" "$LLVM_VERSION"
    return
  fi

  local major
  major=$("$tool" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 | cut -d. -f1)
  if [[ "$major" == "$LLVM_VERSION" ]]; then
    emit_result clang ok
  elif [[ "$(command -v "$tool")" == *"/llvm"* ]]; then
    emit_result clang missing "${YELLOW}✗ (need LLVM $LLVM_VERSION, found ${major:-unknown})${NC}" "$LLVM_VERSION"
  else
    emit_result clang missing "${YELLOW}✗ Expected LLVM Clang (need $LLVM_VERSION)${NC}" "$LLVM_VERSION"
  fi
}

# cargo present + the pinned rust-toolchain.toml version available. Prefer
# `rustup toolchain list` (read-only; never auto-installs) over resolving the
# pin through `cargo`.
check_rust() {
  if command -v rustup &>/dev/null; then
    if rustup toolchain list 2>/dev/null | grep -q "^${PINNED_RUST_VERSION}-"; then
      emit_result cargo ok
    else
      emit_result cargo missing "${YELLOW}✗ (need Rust $PINNED_RUST_VERSION toolchain)${NC}" "$PINNED_RUST_VERSION"
    fi
  elif command -v cargo &>/dev/null; then
    local actual
    actual=$(cargo --version 2>/dev/null | awk '{print $2}')
    if [[ -n "$actual" ]] && version_ge "$actual" "$PINNED_RUST_VERSION"; then
      emit_result cargo ok
    else
      emit_result cargo missing "${YELLOW}✗ (need Rust >= $PINNED_RUST_VERSION, found ${actual:-none})${NC}" "$PINNED_RUST_VERSION"
    fi
  else
    emit_result cargo missing "" "$PINNED_RUST_VERSION"
  fi
}

check_cheadergen() {
  if ! command -v cheadergen &>/dev/null; then
    emit_result cheadergen missing
    return
  fi
  local actual_version
  actual_version=$(cheadergen --version 2>/dev/null | awk '{print $NF}' || echo "unknown")
  if [[ "$actual_version" == "$REQUIRED_CHEADERGEN_VERSION" ]]; then
    emit_result cheadergen ok
  else
    emit_result cheadergen missing "${YELLOW}✗ (need version $REQUIRED_CHEADERGEN_VERSION, found version $actual_version)${NC}" "$REQUIRED_CHEADERGEN_VERSION"
  fi
}

# ============================================
# Main
# ============================================
report_mode || echo -e "\n===== Build Dependencies Checker =====\n"

check_cmd_dep make
check_cmd_dep python3
check_cmd_dep cmake
check_rust
check_clang
check_cmd_dep openssl
check_cmd_dep brew
if should_check_cheadergen; then
  check_cheadergen
fi

# ============================================
# Aggregate report mode: emit records and stop here.
# ============================================
if emit_deps_report; then
  n_ok=$(set -- $DEPS_OK; echo $#)
  n_missing=$(set -- $DEPS_MISSING; echo $#)
  echo "==> [redisearch] checked: $n_ok installed, $n_missing missing"
  return 0 2>/dev/null || exit 0
fi

# ============================================
# Missing Dependencies Handling
# ============================================

if $missing_deps; then
  echo -e "\n${YELLOW}WARNING: Some dependencies are missing or do not meet the required version. \nBuild may fail without these dependencies.${NC}"
  exit_code=1
else
  echo -e "\n${GREEN}All required dependencies are met.${NC}"
  exit_code=0
fi

return "$exit_code" 2>/dev/null || exit "$exit_code"
