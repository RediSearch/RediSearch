#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt dist-upgrade -yqq
$MODE apt install -yqq software-properties-common unzip rsync

# ppa for modern python and gcc10
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE apt update
$MODE apt install -yqq build-essential wget curl make gcc-10 g++-10 openssl libssl-dev cargo binfmt-support lsb-core awscli libclang-dev clang curl
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10

# Install Python 3.8
$MODE apt -y install python3.8 python3.8-venv python3.8-dev python3-venv python3-dev python3-pip

# Set python3 to point to python3.8
$MODE update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 2

source install_cmake.sh $MODE
