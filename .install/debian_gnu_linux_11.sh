#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev python3 python3-dev python3-venv rsync unzip curl libclang-dev
source install_cmake.sh $MODE
