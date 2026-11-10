#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq

# apt.llvm.org has no clang-18 repo for 26.04, but the Ubuntu archive ships clang-format-18.
# check-format.sh calls the unversioned clang-format, so point it at version 18.
$MODE apt install -yqq clang-format-18
$MODE ln -sf /usr/bin/clang-format-18 /usr/local/bin/clang-format
clang-format --version

$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
    python3 python3-venv python3-dev unzip rsync curl
source install_cmake.sh $MODE
