#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt-get -qq update
$MODE apt-get upgrade -yqq
$MODE apt-get dist-upgrade -yqq
$MODE apt-get install software-properties-common -yqq
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE apt update
$MODE apt-get install -yqq git wget make valgrind gcc-9 g++-9
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
source install_cmake.sh $MODE
