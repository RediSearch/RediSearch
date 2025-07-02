#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y make wget git \
    openssl openssl-devel python3 python3-devel which rsync unzip clang curl --nobest --allowerasing

# Find available gcc-toolset versions
GCC_TOOLSETS=(/opt/rh/gcc-toolset-*/enable)

if [ ${#GCC_TOOLSETS[@]} -eq 0 ]; then
    echo "Error: No gcc-toolset versions found in /opt/rh/gcc-toolset-*/" >&2
    exit 1
fi

# Find the highest version available
HIGHEST_VERSION=0
HIGHEST_PATH=""

for toolset in "${GCC_TOOLSETS[@]}"; do
    # Extract version number from path
    VERSION=$(echo "$toolset" | grep -oP 'gcc-toolset-\K\d+')

    if [ -n "$VERSION" ] && [ "$VERSION" -gt "$HIGHEST_VERSION" ]; then
        HIGHEST_VERSION=$VERSION
        HIGHEST_PATH=$toolset
    fi
done

# Check if highest version is at least 13
if [ "$HIGHEST_VERSION" -lt 13 ]; then
    echo "Error: gcc-toolset version $HIGHEST_VERSION is less than required version 13" >&2
    exit 1
fi

# Copy the enable file to profile.d
$MODE cp "$HIGHEST_PATH" "/etc/profile.d/gcc-toolset-$HIGHEST_VERSION.sh"
echo "Installed gcc-toolset-$HIGHEST_VERSION"
