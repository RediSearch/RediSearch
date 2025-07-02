#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y make wget git \
    openssl openssl-devel python3 python3-devel which rsync unzip clang curl --nobest --allowerasing

# Find available gcc-toolset versions
GCC_TOOLSETS=(/opt/rh/gcc-toolset-*/enable)

# Find the highest version using sort and command substitution
HIGHEST_PATH=$(printf "%s\n" "${GCC_TOOLSETS[@]}" | sort -V | tail -1)
HIGHEST_VERSION=$(echo "$HIGHEST_PATH" | grep -oP 'gcc-toolset-\K\d+')

# If no gcc-toolset is found or the highest version is less than 13, install gcc-toolset-13
if [ -z "$HIGHEST_VERSION" || "$HIGHEST_VERSION" -lt 13 ]; then
    $MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++  --nobest --allowerasing
    HIGHEST_PATH="/opt/rh/gcc-toolset-13/enable"
    HIGHEST_VERSION=13
fi

# Copy the enable file to profile.d
$MODE cp "$HIGHEST_PATH" "/etc/profile.d/gcc-toolset-$HIGHEST_VERSION.sh"
echo "Installed gcc-toolset-$HIGHEST_VERSION"
