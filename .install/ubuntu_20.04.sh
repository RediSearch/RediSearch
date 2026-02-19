#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt install -yqq software-properties-common
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE add-apt-repository ppa:deadsnakes/ppa -y
$MODE apt update -yqq

# Install gcc-13 for better ARM64 SVE2 support (required for VectorSimilarity SVS on aarch64)
$MODE apt install -yqq wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-13 g++-13 curl libclang-dev gdb

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 60 --slave /usr/bin/g++ g++ /usr/bin/g++-13
