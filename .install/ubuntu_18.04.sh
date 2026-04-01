#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd update -qq
apt_get_cmd upgrade -yqq
apt_get_cmd dist-upgrade -yqq
apt_get_cmd install -yqq software-properties-common unzip rsync

# ppa for modern python and gcc10
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:git-core/ppa -y
apt_get_cmd update
apt_get_cmd install -yqq build-essential git wget make gcc-10 g++-10 openssl libssl-dev curl libclang-dev clang gdb
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10
