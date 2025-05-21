#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq

$MODE apt install -yqq wget make clang-format gcc python3 python3-venv python3-pip lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
