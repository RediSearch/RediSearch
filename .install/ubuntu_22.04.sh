#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" install -yqq gcc-12 g++-12 git wget build-essential lcov openssl libssl-dev \
    unzip rsync curl gdb
# gcc-12 is installed side-by-side only (versioned names). No update-alternatives:
# flipping /usr/bin/gcc would change the compiler every other checkout on this
# host resolves. build.sh selects gcc-12 explicitly when CC is unset.

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
