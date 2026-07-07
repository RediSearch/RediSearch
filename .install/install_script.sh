#!/usr/bin/env bash
set -eo pipefail

OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    OS='macos'
else
    VERSION=$(grep '^VERSION_ID=' /etc/os-release | sed 's/"//g')
    VERSION=${VERSION#"VERSION_ID="}
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    # AlmaLinux and RHEL are compatible with Rocky Linux install scripts.
    [[ $OS_NAME == 'AlmaLinux' ]] && OS_NAME='Rocky Linux'
    [[ $OS_NAME == 'Red Hat Enterprise Linux' ]] && OS_NAME='Rocky Linux'
    [[ $OS_NAME == 'Rocky Linux' ]] && VERSION=${VERSION%.*} # remove minor version for Rocky Linux
    [[ $OS_NAME == 'Alpine Linux' ]] && VERSION=${VERSION%.*.*} # remove minor and patch version for Alpine Linux
    OS=${OS_NAME,,}_${VERSION}
    OS=$(echo $OS | sed 's/[/ ]/_/g') # replace spaces and slashes with underscores
fi
echo $OS

source ${OS}.sh $MODE
source install_cmake.sh $MODE

# Boost is only useful when the build runs from the same checkout this script
# populates. The CI image builds from /project but jobs build from a fresh
# workspace checkout, so a baked boost is never used (CMake FetchContent
# re-fetches it). Allow the image build to skip it via SKIP_BOOST=1.
if [[ "${SKIP_BOOST:-0}" != 1 ]]; then
    source ./install_boost.sh
fi
# Install Rust and Python here since they're needed on all platforms and
# the installer doesn't rely on any platform-specific tools (e.g. the package manager)
source install_rust.sh
source install_python.sh

# Python test deps (pip-capable venv + RLTest, via uv) live in test_deps/,
# which CI runs as a separate step; run it here too so a plain
# `make bootstrap` also covers the pytest flow (tests/deps/setup_rejson.sh
# builds RedisJSON through readies, which needs a python3 with pip — the
# venv provides one). Runs from the repo root, where the uv project lives.
(cd "$(dirname "${BASH_SOURCE[0]}")/.." && bash .install/test_deps/install_python_deps.sh $MODE)

git config --global --add safe.directory '*'
