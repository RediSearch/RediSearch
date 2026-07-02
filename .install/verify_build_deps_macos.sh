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
# Dependencies
# ============================================
# Define dependencies and their corresponding check methods
mac_os_deps=("make" "python3" "cmake" "cargo" "clang" "openssl" "brew")
mac_os_deps_types=(
    "check_command" # Check for "make"
    "check_command" # Check for "python3"
    "check_command" # Check for "cmake"
    "check_command" # Check for "cargo"
    "check_clang"   # Check for "clang"
    "check_command" # Check for "openssl"
    "check_command" # Check for "brew"
)

# cheadergen is only needed when regenerating Rust C headers.
# REDISEARCH_GENERATE_HEADERS=0 builds from checked-in headers.
if should_check_cheadergen; then
  mac_os_deps+=("cheadergen")
  mac_os_deps_types+=("check_cheadergen")
fi

# Function to check if a command is available
check_command() {
  local cmd=$1
  printf "%-20s" "$cmd"

  if ! command -v "$cmd" &>/dev/null; then
    echo -e "${RED}✗${NC}"
    missing_deps=true
  else
    echo -e "${GREEN}✓${NC}"
  fi
}

check_clang() {
    printf "%-20s" "clang"

    if command -v clang &>/dev/null; then
        clang_path=$(command -v clang)
        if [[ "$clang_path" == *"/llvm"* ]]; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${YELLOW}✗ Expected LLVM Clang${NC}"
            missing_deps=true
        fi
    else
        echo -e "${RED}✗${NC}"
        missing_deps=true
    fi
}

check_cheadergen() {
  local cmd=$1
  printf "%-20s" "$cmd"

  if ! command -v "$cmd" &>/dev/null; then
    echo -e "${RED}✗${NC}"
    missing_deps=true
    return
  fi

  local actual_version
  actual_version=$("$cmd" --version 2>/dev/null | awk '{print $NF}' || echo "unknown")
  if [[ "$actual_version" == "$REQUIRED_CHEADERGEN_VERSION" ]]; then
    echo -e "${GREEN}✓${NC}"
  else
    echo -e "${YELLOW}✗ (need version $REQUIRED_CHEADERGEN_VERSION, found version $actual_version)${NC}"
    missing_deps=true
  fi
}

# ============================================
# Main Loop
# ============================================
# Print header
echo -e "\n===== Build Dependencies Checker =====\n"

missing_deps=false

for i in "${!mac_os_deps[@]}"; do
  dep="${mac_os_deps[$i]}"
  check_function="${mac_os_deps_types[$i]}"
  $check_function "$dep"
done

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
