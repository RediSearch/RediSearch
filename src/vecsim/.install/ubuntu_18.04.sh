#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt-get update -qq
$MODE apt-get upgrade -yqq
$MODE apt-get dist-upgrade -yqq
$MODE apt install -yqq software-properties-common
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE apt update
$MODE apt-get install -yqq git wget make gcc-11 g++-11 libc6-dbg
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11

wget https://sourceware.org/pub/valgrind/valgrind-3.18.0.tar.bz2
tar -xjf valgrind-3.18.0.tar.bz2
cd valgrind-3.18.0
./configure
make
$MODE make install
cd ..

source install_cmake.sh $MODE
