#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
APT_GET_LOCK_TIMEOUT_SECONDS="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}"

apt_get_cmd() {
    $MODE apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" "$@"
}

apt_get_cmd update -qq
apt_get_cmd upgrade -yqq

# Provides the add-apt-repository command
apt_get_cmd install -yqq software-properties-common

$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:deadsnakes/ppa -y

apt_get_cmd install -yqq wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev gdb

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
# Align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 60

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
