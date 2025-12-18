#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq

$MODE apt install -yqq wget make clang-format gcc python3 python3-venv python3-pip lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-10 g++-10 cargo libclang-dev clang curl

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 60 --slave /usr/bin/g++ g++ /usr/bin/g++-10

source install_cmake.sh $MODE
