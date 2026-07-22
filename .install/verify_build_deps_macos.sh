#!/usr/bin/env bash
#
# macOS build-dependency checks. Sourced by verify_build_deps.sh on Darwin, and
# NOT meant to run standalone: it relies on the colors, version pins
# (LLVM_VERSION / PINNED_RUST_VERSION), report helpers (emit_result /
# emit_deps_report / report_mode / get_llvm_tool / get_llvm_major),
# should_check_cheadergen and REQUIRED_CHEADERGEN_VERSION that the parent
# defines before sourcing this file.

if ! declare -F emit_result >/dev/null 2>&1; then
  echo "verify_build_deps_macos.sh must be sourced by verify_build_deps.sh" >&2
  return 1 2>/dev/null || exit 1
fi

# ============================================
# Checkers
# ============================================

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
  local tool
  tool=$(get_llvm_tool clang)
  if [[ -z "$tool" ]]; then
    emit_result clang missing "${RED}✗${NC}" "$LLVM_VERSION"
    return
  fi

  # clang-<major> is the pinned major by name — trust it without --version.
  if [[ "$tool" == "clang-${LLVM_VERSION}" ]]; then
    emit_result clang ok
    return
  fi

  local major
  major=$(get_llvm_major "$tool")
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
