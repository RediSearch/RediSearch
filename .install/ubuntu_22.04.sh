#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq gcc-12 g++-12 git wget build-essential lcov openssl libssl-dev python3 python3-venv python3-dev unzip rsync curl
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60 --slave /usr/bin/g++ g++ /usr/bin/g++-12
# align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 60
source install_cmake.sh $MODE
source install_locales.sh $MODE
