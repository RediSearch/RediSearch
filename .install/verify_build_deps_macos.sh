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
mac_os_deps_types=("command" "command" "command" "command" "command" "command" "command")

# Function to check if a command is available
check_command() {
  command -v "$1" &> /dev/null
}

# Check brew packages on macOS
check_package() {
    brew list --formula | grep -q "^$package\$" || brew list --cask | grep -q "^$package\$"
}

# ============================================
# OS-Specific Dependencies
# ============================================

# Print header
echo -e "\n===== Build Dependencies Checker =====\n"

missing_deps=false

# Common dependencies with their minimum versions where applicable
check_dependency() {
  local dep="$1"
  local check_type="$2"
  local min_version="$3"

  printf "%-20s" "$dep"

  if [ "$check_type" = "command" ]; then
    if ! check_command "$dep"; then
      echo -e "${RED}✗${NC}"
      missing_deps=true
    elif [ -n "$min_version" ]; then
      # Check version if minimum is specified
      if check_compiler_min_version "$dep" "$min_version"; then
        echo -e "${GREEN}✓${NC}"
      else
        actual_version=$(get_compiler_version "$dep")
        echo -e "${YELLOW}✗ (need version >= $min_version, found version $actual_version)${NC}"
        missing_deps=true
      fi
    else
      echo -e "${GREEN}✓${NC}"
    fi
  elif [ "$check_type" = "package" ]; then
    if check_package "$dep"; then
      echo -e "${GREEN}✓${NC}"
    else
      echo -e "${RED}✗${NC}"
      missing_deps=true
    fi
  else
    echo -e "${YELLOW} (no method defined)${NC}"
    missing_deps=true
  fi
}

for i in "${!mac_os_deps[@]}"; do
  dep="${mac_os_deps[$i]}"
  check_type="${mac_os_deps_types[$i]}"

  # Check if the dependency is installed
  check_dependency "$dep" "$check_type"
done
