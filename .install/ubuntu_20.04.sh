#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq wget make clang-format gcc python3 python3-venv python3-pip lcov git openssl libssl-dev
source install_cmake.sh $MODE
