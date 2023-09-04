#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt update -qq
apt upgrade -yqq
apt dist-upgrade -yqq
apt install -yqq software-properties-common
add-apt-repository ppa:ubuntu-toolchain-r/test -y
apt update
apt install -yqq git wget make valgrind gcc-9 g++-9
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
source install_cmake.sh
