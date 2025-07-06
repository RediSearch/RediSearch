#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt dist-upgrade -yqq
$MODE apt install -yqq software-properties-common unzip rsync

# ppa for modern python and gcc10
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:git-core/ppa -y
$MODE add-apt-repository ppa:deadsnakes/ppa -y
$MODE apt update -yqq
$MODE apt install -yqq build-essential git wget make gcc-10 g++-10 openssl libssl-dev curl libclang-dev clang
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10

# Install Python 3.12
$MODE apt -y install python3.12 python3.12-venv python3.12-dev python3-venv python3-dev

# Set python3 to point to python3.12
$MODE update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2
