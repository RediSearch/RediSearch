#!/usr/bin/env bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

# Check if rustup is installed, if not, install it
if ! command -v rustup &> /dev/null; then
    source ./.install/install_rust.sh
fi
./.install/test_deps/install_rust_deps.sh
./.install/test_deps/install_python_deps.sh
