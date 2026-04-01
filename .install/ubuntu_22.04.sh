#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" install -yqq gcc-12 g++-12 git wget build-essential lcov openssl libssl-dev \
    unzip rsync curl gdb perl
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60 --slave /usr/bin/g++ g++ /usr/bin/g++-12
# Align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 60

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
