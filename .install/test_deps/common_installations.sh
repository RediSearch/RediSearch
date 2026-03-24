#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

./.install/test_deps/install_rust_deps.sh
./.install/test_deps/install_python_deps.sh
