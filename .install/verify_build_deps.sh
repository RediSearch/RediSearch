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
    echo "$ID"
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
  ["libssl-dev"]="package" # Verify using dpkg -l
)

declare -A rocky_dependencies=(
  ["openssl-devel"]="package" # Verify using rpm -q
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
else
  echo -e "${RED}Unsupported operating system.${NC}"
  exit 1
fi

# Define installation scripts for specific dependencies
declare -A install_scripts=(
  ["cargo"]="./install/install_rust.sh"
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

# Function to check if a package is installed (Rocky/RHEL)
check_package_rpm() {
  rpm -q "$1" &> /dev/null
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
    elif [[ "$OS" == "rocky" ]] && check_package_rpm "$dep"; then
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

  # Then suggest apt-get or dnf/yum for the rest
  if [ ${#missing_deps_apt[@]} -gt 0 ]; then
    echo -e "\n${BLUE}For other dependencies, run:${NC}"
    if [[ "$OS" == "ubuntu"  || "$OS" == "debian" ]]; then
      echo -e "sudo apt install \\"
    elif [[ "$OS" == "rocky" ]]; then
      echo -e "sudo dnf install \\"
    fi
    for dep in "${missing_deps_apt[@]}"; do
      echo -e "  $dep \\"
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
