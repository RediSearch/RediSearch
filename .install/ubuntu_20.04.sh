#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd "$MODE" update
apt_get_cmd "$MODE" upgrade -y

# Provides the add-apt-repository command
apt_get_cmd "$MODE" install -y software-properties-common

$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:deadsnakes/ppa -y

apt_get_cmd "$MODE" install -y wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev gdb

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
# Align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 60

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
