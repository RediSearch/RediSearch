#!/bin/bash

# Set colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
        fi
    else
        echo -e "${RED}✗${NC}"
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
