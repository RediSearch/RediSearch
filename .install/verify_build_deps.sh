#!/bin/bash
# filepath: /home/ubuntu/RediSearch/scripts/check_build_deps.sh
set -e
# Set colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
else
  echo -e "${RED}Unsupported operating system.${NC}"
  exit 1
fi

# Define installation scripts for specific dependencies
declare -A install_scripts=(
  ["cargo"]=".install/install_rust.sh"
  # Add more installation scripts as needed
)

# Print header
echo -e "\n===== Build Dependencies Checker =====\n"

# Function to check if a command is available
check_command() {
  command -v "$1" &> /dev/null
}

# Function to check if a package is installed (Ubuntu/Debian)
check_package_deb() {
  dpkg -l | grep -q " $1 " || dpkg -l | grep -q " $1:"
}

# Function to check if a package is installed (Rocky/RHEL or Amazon Linux)
check_package_rhel() {
  if [[ "$OS" == "amzn2" ]]; then
    yum list installed "$1" &> /dev/null
  elif [[ "$OS" == "amzn2023" ]]; then
    dnf list installed "$1" &> /dev/null
  else
    rpm -q "$1" &> /dev/null
  fi
}

check_package_alpine() {
  apk info -e "$1" &> /dev/null
}

# Function to check if running in a Docker container
is_docker() {
  [ -f /.dockerenv ] || grep -q docker /proc/1/cgroup 2>/dev/null
}

# Arrays to store missing dependencies
missing_deps_with_scripts=()
missing_deps_apt=()

# Check each dependency
for dep in "${!dependencies[@]}"; do
  printf "%-20s" "$dep"

  verify_method=${dependencies[$dep]}

  # Check based on verification method
  if [[ "$verify_method" == "command" ]] && check_command "$dep"; then
    echo -e "${GREEN}✓${NC}"
  elif [[ "$verify_method" == "package" ]]; then
    if [[ "$OS" == "ubuntu" || "$OS" == "debian" ]] && check_package_deb "$dep"; then
      echo -e "${GREEN}✓${NC}"
    elif [[ "$OS" == "rocky" || "$OS" == "amzn2" || "$OS" == "amzn2023" ]] && check_package_rhel "$dep"; then
      echo -e "${GREEN}✓${NC}"
    elif [[ "$OS" == "alpine" ]] && check_package_alpine "$dep"; then
      echo -e "${GREEN}✓${NC}"
    else
      echo -e "${RED}✗${NC}"
      missing_deps_apt+=("$dep")
    fi
  else
    echo -e "${RED}✗${NC}"

    # Check if we have an installation script for this dependency
    if [[ -n "${install_scripts[$dep]}" ]]; then
      missing_deps_with_scripts+=("$dep")
    else
      missing_deps_apt+=("$dep")
    fi
  fi
done

# Print installation instructions if there are missing dependencies
if [ ${#missing_deps_with_scripts[@]} -gt 0 ] || [ ${#missing_deps_apt[@]} -gt 0 ]; then
  echo -e "\n${RED}Missing dependencies:${NC}"

  # First suggest installation scripts
  if [ ${#missing_deps_with_scripts[@]} -gt 0 ]; then
    echo -e "\n${BLUE}Run the following installation scripts:${NC}"
    for dep in "${missing_deps_with_scripts[@]}"; do
      script_path=${install_scripts[$dep]}
      echo -e "${BLUE}For $dep:${NC} $script_path"
    done
  fi

  # Suggest using the all-in-one installation script
  echo -e "\n${YELLOW}Alternatively, you can run the following script to install all dependencies:${NC}"
  echo -e "${BLUE}.install/install_script.sh${NC}"

  echo -e "\n${YELLOW}WARNING: Build may fail without these dependencies.${NC}"

  # Check if running in Docker - if so, don't exit
  if is_docker; then
    echo -e "\n${YELLOW}Running in Docker environment - continuing despite missing dependencies.${NC}"
    exit_code=0
  else
    exit_code=1
  fi
else
  echo -e "\n${GREEN}All dependencies are installed.${NC}"
  exit_code=0
fi

# Return the status code but don't exit in Docker
if ! is_docker ; then
  exit $exit_code
fi
exit $exit_code
