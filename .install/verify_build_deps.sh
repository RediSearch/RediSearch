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
source "$SCRIPT_DIR/version_compare.sh"

# Toolchain version pins, read from the same files the install path uses so the
# check can't drift from what bootstrap installs.
source "$SCRIPT_DIR/LLVM_VERSION.sh"
PINNED_RUST_VERSION=$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' \
  "$REPO_ROOT/rust-toolchain.toml" | head -1)

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
# check-deps report state (shared cross-module bootstrap contract)
# ============================================
# This script never installs — it only reports. The shared `make bootstrap
# check-deps` (see Makefile) runs it to learn what each module needs.
#
# When DEPS_REPORT_FILE is set (aggregate mode), machine-readable records are
# appended and the per-dependency human list is suppressed, so the outer
# bootstrap can print one deduped union across all modules. Record format:
#   ok <name> | missing <name>[:<version>] | opt_ok <name> | opt_missing <name>
missing_deps=false
DEPS_OK=""
DEPS_MISSING=""
DEPS_OPT_OK=""
DEPS_OPT_MISSING=""

# Optional = installed by bootstrap but NOT needed for the default (non-LTO)
# build/run, so their absence must not fail the check. ld.lld is only used as
# the LTO linker; a plain `make build` uses the system linker.
OPTIONAL_DEPS="ld.lld"
is_optional_dep() { case " $OPTIONAL_DEPS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

report_mode() { [ -n "${DEPS_REPORT_FILE:-}" ]; }

# emit_result <name> ok
# emit_result <name> missing [human_message] [report_version]
# Prints one aligned human line (skipped in aggregate mode) and records the
# outcome. Optional deps (OPTIONAL_DEPS) go to their own bucket and never flip
# missing_deps. human_message defaults to a red ✗; pass a yellow message for
# present-but-wrong-version failures.
emit_result() {
  local dep=$1 status=$2 msg=${3:-} vtag=${4:-}
  if ! report_mode; then
    printf "%-20s" "$dep"
    if [[ "$status" == ok ]]; then
      echo -e "${GREEN}✓${NC}"
    else
      echo -e "${msg:-${RED}✗${NC}}"
    fi
  fi
  if [[ "$status" == ok ]]; then
    if is_optional_dep "$dep"; then DEPS_OPT_OK="$DEPS_OPT_OK $dep"; else DEPS_OK="$DEPS_OK $dep"; fi
  elif is_optional_dep "$dep"; then
    # Match the RTS report convention: optional-missing records carry no version tag.
    DEPS_OPT_MISSING="$DEPS_OPT_MISSING $dep"
  else
    local tag=$dep
    [[ -n "$vtag" ]] && tag="$dep:$vtag"
    DEPS_MISSING="$DEPS_MISSING $tag"
    missing_deps=true
  fi
}

# Append the machine-readable records in aggregate mode. Returns 0 (and writes)
# only when DEPS_REPORT_FILE is set, so callers can `emit_deps_report && exit 0`.
emit_deps_report() {
  report_mode || return 1
  local p
  for p in $DEPS_OK;          do echo "ok $p"           >> "$DEPS_REPORT_FILE"; done
  for p in $DEPS_MISSING;     do echo "missing $p"      >> "$DEPS_REPORT_FILE"; done
  for p in $DEPS_OPT_OK;      do echo "opt_ok $p"       >> "$DEPS_REPORT_FILE"; done
  for p in $DEPS_OPT_MISSING; do echo "opt_missing $p"  >> "$DEPS_REPORT_FILE"; done
  return 0
}

# ============================================
# LLVM toolchain resolution (shared by the Linux loop and the macOS clang check)
# ============================================

# Extract the major version from an LLVM tool's --version output. clang prints
# "clang version 21.1.8", lld prints "LLD 21.1.8" (no "version" word), so match
# the first X.Y[.Z] token rather than keying off a label.
get_llvm_major() {
  "$1" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 | cut -d. -f1
}

# Resolve the LLVM-toolchain binary the build uses: build.sh prefers the
# version-suffixed name (clang-$LLVM_VERSION), falling back to the unversioned
# one whose major must match. Uses `command -v` directly (not check_command) so
# it also works when sourced on macOS, where check_command is never defined.
get_llvm_tool() {
  local base=$1
  if command -v "${base}-${LLVM_VERSION}" &>/dev/null; then
    echo "${base}-${LLVM_VERSION}"
  elif command -v "$base" &>/dev/null; then
    echo "$base"
  fi
}

# ============================================
# OS Detection
# ============================================

# Check if the OS is macOS (Darwin)
if [[ "$(uname -s)" == "Darwin" ]]; then
    source "$SCRIPT_DIR/verify_build_deps_macos.sh"
    exit $?
fi

# Function to detect the operating system
detect_os() {
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    if [[ "$ID" == "amzn" ]]; then
      if [[ "$VERSION_ID" == "2" ]]; then
        echo "amzn2"
      elif [[ "$VERSION_ID" == "2023" ]]; then
        echo "amzn2023"
      fi
    # A workaround for GitHub Actions
    elif [[ "$ID" == "NotpineForGHA" ]]; then
      echo "alpine"
    else
      echo "$ID"
    fi
  else
    echo "unknown"
  fi
}

# Detect the OS
OS=$(detect_os)

# ============================================
# Package Checker Functions
# ============================================

# This also serves as the list of supported OSes
declare -A os_package_checkers=(
  ["ubuntu"]="check_package_dpkg"
  ["debian"]="check_package_dpkg"
  ["rocky"]="check_package_rpm"
  ["almalinux"]="check_package_rpm"
  ["rhel"]="check_package_rpm"
  ["amzn2023"]="check_package_dnf"
  ["alpine"]="check_package_apk"
  ["azurelinux"]="check_package_tdnf"
)

# Early bailout if the OS is not supported
if [[ -z "${os_package_checkers[$OS]}" ]]; then
  echo -e "${YELLOW}Error: Unsupported operating system: $OS.${NC}"
  echo -e "${YELLOW}Abort dependency check.${NC}"
  exit 1
fi

# Function to check if a command is available
check_command() {
  command -v "$1" &> /dev/null
}

# ubuntu and debian
check_package_dpkg() {
  dpkg -l | grep -q " $1 " || dpkg -l | grep -q " $1:"
}

# rhel
check_package_rpm() {
  rpm -q "$1" &> /dev/null
}

# amzn2023
check_package_dnf() {
  dnf list installed "$1" &> /dev/null
}

# alpine
check_package_apk() {
  apk info -e "$1" &> /dev/null
}

# azure linux
check_package_tdnf() {
  tdnf list installed "$1" &> /dev/null
}

# ============================================
# Version Check Functions
# ============================================

# Define dependencies that need version checking
# Note: check function is called with the following parameters:
# $1 - actual version
# $2 - min version
# $3 - max version

# The check function should return:
# 0 - version is ok
# 1 - version is below minimum
# 2 - version is above maximum

# It is not mandatory to implement checks for both min and max
# [<dep>] = "<get_version_function> <check_function> <min_version> [<max_version>]"
declare -A version_checks=(
  ["gcc"]="get_compiler_version check_gcc_min_version 10"
  ["g++"]="get_compiler_version check_gpp_min_version 10"
  ["cmake"]="get_cmake_version check_cmake_version 3.25"
)

# ==== Version Getters ====

get_compiler_version() {
  local program=$1
  "$program" -dumpversion 2>/dev/null || echo "unknown"
}

# extract the version in the format X.Y
get_cmake_version() {
  cmake --version | grep "cmake version" | sed -E 's/.*cmake version ([0-9]+\.[0-9]+).*/\1/'
}

get_cheadergen_version() {
  cheadergen --version 2>/dev/null | awk '{print $NF}' || echo "unknown"
}

# ==== Version Checkers ====

check_min_version() {
  version_ge "$1" "$2"
}

check_max_version() {
  version_ge "$2" "$1"
}

# ====  Specialized Checkers ====

check_gcc_min_version() {
  local actual_version="$1"
  local min_version="$2"
  check_min_version "$actual_version" "$min_version"
}

check_gpp_min_version() {
  local actual_version="$1"
  local min_version="$2"
  check_min_version "$actual_version" "$min_version"
}

check_cmake_version() {
  local actual_version="$1"
  local min_version="$2"
  check_min_version "$actual_version" "$min_version"
}

# ============================================
# OS-Specific Dependencies
# ============================================

# Common dependencies, keyed to a verify_method dispatched in the loop below.
declare -A common_dependencies=(
  ["make"]="command"
  ["gcc"]="command"
  ["g++"]="command"
  ["python3"]="command"
  ["cmake"]="command"
  ["cargo"]="rust"
  ["clang"]="llvm"    # required: bindgen/libclang need it, LTO uses it as CC
  ["ld.lld"]="llvm"   # LTO linker only -> optional (see OPTIONAL_DEPS)
)

# cheadergen is only needed when regenerating Rust C headers.
# REDISEARCH_GENERATE_HEADERS=0 builds from checked-in headers.
if should_check_cheadergen; then
  common_dependencies["cheadergen"]="cheadergen"
fi

# Define OS-specific dependencies
declare -A ubuntu_dependencies=(
  ["libssl-dev"]="package"
)

declare -A rocky_dependencies=(
  ["openssl-devel"]="package"
)

declare -A amzn2023_dependencies=(
  ["openssl-devel"]="package"
)

declare -A alpine_dependencies=(
  ["openssl-dev"]="package"
  ["bsd-compat-headers"]="package"
)

declare -A microsoft_dependencies=(
  ["openssl-devel"]="package"
  ["binutils"]="package"
  ["glibc-devel"]="package"
  ["kernel-headers"]="package"
)

# ============================================
# Merge Dependencies
# ============================================

# Merge common and OS-specific dependencies
declare -A dependencies

# Add common dependencies
for key in "${!common_dependencies[@]}"; do
  dependencies["$key"]="${common_dependencies[$key]}"
done

# Add OS-specific dependencies
if [[ "$OS" == "ubuntu" || "$OS" == "debian" ]]; then
  for key in "${!ubuntu_dependencies[@]}"; do
    dependencies["$key"]="${ubuntu_dependencies[$key]}"
  done
elif [[ "$OS" == "rocky" || "$OS" == "almalinux" || "$OS" == "rhel" ]]; then
  for key in "${!rocky_dependencies[@]}"; do
    dependencies["$key"]="${rocky_dependencies[$key]}"
  done
elif [[ "$OS" == "amzn2023" ]]; then
  for key in "${!amzn2023_dependencies[@]}"; do
    dependencies["$key"]="${amzn2023_dependencies[$key]}"
  done
elif [[ "$OS" == "alpine" ]]; then
  for key in "${!alpine_dependencies[@]}"; do
    dependencies["$key"]="${alpine_dependencies[$key]}"
  done
elif [[ "$OS" == "azurelinux" ]]; then
  for key in "${!microsoft_dependencies[@]}"; do
    dependencies["$key"]="${microsoft_dependencies[$key]}"
  done
else
  echo -e "${YELLOW}Warning: OS '$OS' does not have special dependencies.${NC}"
fi

# ============================================
# Dependency Verification
# ============================================

# Print header
report_mode || echo -e "\n===== Build Dependencies Checker =====\n"

# Check each dependency
for dep in "${!dependencies[@]}"; do
  verify_method=${dependencies[$dep]}

  # Check based on verification method
  if [[ "$verify_method" == "command" ]]; then
    if ! check_command "$dep"; then
      emit_result "$dep" missing
    elif [[ -n "${version_checks[$dep]}" ]]; then
      # dep exists, run its version check
      read -r get_version check_func min_version max_version <<< "${version_checks[$dep]}"
      actual_version=$($get_version "$dep")
      $check_func $actual_version $min_version $max_version
      result=$?
      if [[ "$result" -eq 1 ]]; then # below min version
        emit_result "$dep" missing "${YELLOW}✗ (need version >= $min_version, found version $actual_version)${NC}" "$min_version"
      elif [[ "$result" -eq 2 ]]; then # exceeded max version
        emit_result "$dep" missing "${YELLOW}✗ (need version < $max_version, found version $actual_version)${NC}" "$min_version"
      else
        emit_result "$dep" ok
      fi
    else
      # No version check needed
      emit_result "$dep" ok
    fi
  elif [[ "$verify_method" == "package" ]]; then
    # Lookup the package checker for the current OS and call it dynamically
    package_checker=${os_package_checkers["$OS"]}
    if $package_checker "$dep"; then
      emit_result "$dep" ok
    else
      emit_result "$dep" missing
    fi
  elif [[ "$verify_method" == "llvm" ]]; then
    tool=$(get_llvm_tool "$dep")
    if [[ -z "$tool" ]]; then
      emit_result "$dep" missing "${RED}✗ (LLVM $LLVM_VERSION toolchain not found)${NC}" "$LLVM_VERSION"
    elif [[ "$tool" == "${dep}-${LLVM_VERSION}" ]]; then
      # A version-suffixed binary (clang-21, ld.lld-21) is the pinned major by
      # name — trust it without running --version. Matters for ld.lld: the
      # bare `lld` driver prints no version, so probing it reports "unknown".
      emit_result "$dep" ok
    else
      # Fell back to the unversioned binary; confirm its major.
      actual_major=$(get_llvm_major "$tool")
      if [[ "$actual_major" == "$LLVM_VERSION" ]]; then
        emit_result "$dep" ok
      else
        emit_result "$dep" missing "${YELLOW}✗ (need LLVM $LLVM_VERSION, found ${actual_major:-unknown})${NC}" "$LLVM_VERSION"
      fi
    fi
  elif [[ "$verify_method" == "rust" ]]; then
    if check_command rustup; then
      # Read-only: is the pinned toolchain installed? `rustup toolchain list`
      # never triggers an auto-install (unlike resolving the pin via `cargo`).
      if rustup toolchain list 2>/dev/null | grep -q "^${PINNED_RUST_VERSION}-"; then
        emit_result "$dep" ok
      else
        emit_result "$dep" missing "${YELLOW}✗ (need Rust $PINNED_RUST_VERSION toolchain)${NC}" "$PINNED_RUST_VERSION"
      fi
    elif check_command cargo; then
      # No rustup (e.g. a distro cargo): fall back to a version floor.
      actual_version=$(cargo --version 2>/dev/null | awk '{print $2}')
      if [[ -n "$actual_version" ]] && version_ge "$actual_version" "$PINNED_RUST_VERSION"; then
        emit_result "$dep" ok
      else
        emit_result "$dep" missing "${YELLOW}✗ (need Rust >= $PINNED_RUST_VERSION, found ${actual_version:-none})${NC}" "$PINNED_RUST_VERSION"
      fi
    else
      emit_result "$dep" missing "" "$PINNED_RUST_VERSION"
    fi
  elif [[ "$verify_method" == "cheadergen" ]]; then
    if ! check_command "$dep"; then
      emit_result "$dep" missing
    else
      actual_version=$(get_cheadergen_version)
      if [[ "$actual_version" == "$REQUIRED_CHEADERGEN_VERSION" ]]; then
        emit_result "$dep" ok
      else
        emit_result "$dep" missing "${YELLOW}✗ (need version $REQUIRED_CHEADERGEN_VERSION, found version $actual_version)${NC}" "$REQUIRED_CHEADERGEN_VERSION"
      fi
    fi
  else # no method is defined for this dependency
    emit_result "$dep" missing "${YELLOW} (no method defined)${NC}"
  fi
done

# ============================================
# Aggregate report mode: emit records for the outer bootstrap and stop here.
# ============================================
if emit_deps_report; then
  n_ok=$(set -- $DEPS_OK; echo $#)
  n_missing=$(set -- $DEPS_MISSING; echo $#)
  echo "==> [redisearch] checked: $n_ok installed, $n_missing missing"
  exit 0
fi

# ============================================
# Missing Dependencies Handling
# ============================================

# Optional deps (installed by bootstrap, not needed for a default build) are
# reported separately and never fail the check.
if [[ -n "$DEPS_OPT_MISSING" ]]; then
  echo -e "\n${YELLOW}Optional (not installed; needed only for LTO/extra features):${NC}"
  for p in $DEPS_OPT_MISSING; do echo -e "    ${p%%:*}"; done
fi

# Print installation instructions if there are missing dependencies
if $missing_deps; then
  echo -e "\n${YELLOW}WARNING: Some dependencies are missing or do not meet the required version. \nBuild may fail without these dependencies.${NC}"
  exit_code=1
else
  echo -e "\n${GREEN}All required dependencies are met.${NC}"
  exit_code=0
fi

# Suggest using the all-in-one installation script
echo -e "\n\033[0;36mTo install or inspect dependencies, check the following script:\033[0m"
is_docker() {
  [[ -f /.dockerenv ]] || grep -q docker /proc/1/cgroup 2>/dev/null
}
mode=$(is_docker && echo "" || echo "sudo")
echo -e "cd .install && ./install_script.sh ${mode}"

exit $exit_code
