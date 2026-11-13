#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
sudo apt install -y wget lsb-release gnupg

# Add the LLVM APT repo for the latest clang-18 version
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
$MODE ./llvm.sh 18
clang-format-18 --version

$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
    python3 python3-venv python3-dev unzip rsync curl
source install_cmake.sh $MODE
