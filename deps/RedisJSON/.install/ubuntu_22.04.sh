#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
  python3 python3-pip python3-venv python3-dev unzip rsync libclang-dev clang curl
source install_cmake.sh $MODE
