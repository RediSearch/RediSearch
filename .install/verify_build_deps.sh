#!/bin/bash

# Set colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================
# OS Detection
# ============================================

# Check if the OS is macOS (Darwin)
if [[ "$(uname -s)" == "Darwin" ]]; then
    source .install/verify_build_deps_macos.sh
    exit $?
fi

# Function to detect the operating system
detect_os() {
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    if [[ "$ID" == "amzn" ]]; then
      if [[ "$VERSION_ID" == "2" ]]; then
        echo "amzn2"
      elif [[ "$VERSION_ID" == "2023" ]]; then
        echo "amzn2023"
      fi
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
  ["amzn2"]="check_package_yum"
  ["amzn2023"]="check_package_dnf"
  ["alpine"]="check_package_apk"
  ["mariner"]="check_package_tdnf"
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

# amzn2
check_package_yum() {
  yum list installed "$1" &> /dev/null
}

# amzn2023
check_package_dnf() {
  dnf list installed "$1" &> /dev/null
}

# alpine
check_package_apk() {
  apk info -e "$1" &> /dev/null
}

# mariner and azure linux
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
  cmake --version | grep -oP 'cmake version \K[0-9.]+' | cut -d. -f1,2
}

# ==== Version Checkers ====

check_min_version() {
    local actual_version="$1"
    local min_version="$2"

    # Sort the versions from min to max, expecting the first one to be the minimum
    [ "$(printf '%s\n' "$min_version" "$actual_version" | sort -V | head -n 1)" = "$min_version" ]
}

check_max_version() {
    local actual_version="$1"
    local max_version="$2"

    # Sort the versions from min to max, expecting the first one to be the actual_version
    [ "$(printf '%s\n' "$max_version" "$actual_version" | sort -V | head -n 1)" = "$actual_version" ]
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

# Define common dependencies
declare -A common_dependencies=(
  ["make"]="command"       # Verify using command -v
  ["gcc"]="command"        # Verify using command -v
  ["g++"]="command"        # Verify using command -v
  ["python3"]="command"    # Verify using command -v
  ["cmake"]="command"      # Verify using command -v
  ["cargo"]="command"      # Verify using command -v
)

# Define OS-specific dependencies
declare -A ubuntu_dependencies=(
  ["libssl-dev"]="package"
)

declare -A rocky_dependencies=(
  ["openssl-devel"]="package"
)

declare -A amzn2_dependencies=(
  ["openssl11-devel"]="package"
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
elif [[ "$OS" == "rocky" ]]; then
  for key in "${!rocky_dependencies[@]}"; do
    dependencies["$key"]="${rocky_dependencies[$key]}"
  done
elif [[ "$OS" == "amzn2" ]]; then
  for key in "${!amzn2_dependencies[@]}"; do
    dependencies["$key"]="${amzn2_dependencies[$key]}"
  done
elif [[ "$OS" == "amzn2023" ]]; then
  for key in "${!amzn2023_dependencies[@]}"; do
    dependencies["$key"]="${amzn2023_dependencies[$key]}"
  done
elif [[ "$OS" == "alpine" ]]; then
  for key in "${!alpine_dependencies[@]}"; do
    dependencies["$key"]="${alpine_dependencies[$key]}"
  done
elif [[ "$OS" == "mariner" || "$OS" == "azurelinux" ]]; then
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
echo -e "\n===== Build Dependencies Checker =====\n"

# Arrays to store missing dependencies
missing_deps=false

# Check each dependency
for dep in "${!dependencies[@]}"; do
  printf "%-20s" "$dep"

  verify_method=${dependencies[$dep]}

  # Check based on verification method
  if [[ "$verify_method" == "command" ]]; then
    # missing dep
    if ! check_command "$dep"; then
      echo -e "${RED}✗${NC}"
      missing_deps=true
    else
      # dep exist, check if version verification is needed
      if [[ -n "${version_checks[$dep]}" ]]; then
        # Extract version check function and minimum version
        read -r get_version check_func min_version max_version <<< "${version_checks[$dep]}"

        # Run the check function
        actual_version=$($get_version "$dep")
        $check_func $actual_version $min_version $max_version
        result=$?
        if [ "$result" -eq 1 ]; then # below min version
          echo -e "${YELLOW}✗ (need version >= $min_version, found version $actual_version)${NC}"
          missing_deps=true
        elif [ "$result" -eq 2 ]; then # exceeded max version
          echo -e "${YELLOW}✗ (need version < $max_version, found version $actual_version)${NC}"
          missing_deps=true
        else
          echo -e "${GREEN}✓${NC}"
        fi
      else
        # No version check needed
        echo -e "${GREEN}✓${NC}"
      fi
    fi
  elif [[ "$verify_method" == "package" ]]; then
    # Lookup the package checker for the current OS
    package_checker=${os_package_checkers["$OS"]}

    # Call the package checker dynamically
    if $package_checker "$dep"; then
      echo -e "${GREEN}✓${NC}"
    else
      echo -e "${RED}✗${NC}"
      missing_deps=true
    fi
  else # no method is defined for this dependency
    echo -e "${YELLOW} (no method defined)${NC}"
    missing_deps=true
  fi
done

# ============================================
# Missing Dependencies Handling
# ============================================

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
  [ -f /.dockerenv ] || grep -q docker /proc/1/cgroup 2>/dev/null
}
mode=$(is_docker && echo "" || echo "sudo")
echo -e "cd .install && ./install_script.sh ${mode}"

exit $exit_code
