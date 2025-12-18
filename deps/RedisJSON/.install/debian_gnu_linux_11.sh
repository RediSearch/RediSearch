#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt install -yqq git wget curl build-essential lcov openssl libssl-dev python3 python3-venv python3-pip \
  rsync unzip cargo libclang-dev clang

source install_cmake.sh $MODE
