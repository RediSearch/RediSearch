#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt-get update -qq
$MODE apt install -yqq software-properties-common
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE apt update
$MODE apt-get install -yqq wget gcc-11 g++-11 make clang-format valgrind python3-pip lcov git
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
# align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 60
source install_cmake.sh $MODE
