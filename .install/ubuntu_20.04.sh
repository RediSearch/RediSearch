#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt-get update -qq
$MODE apt-get install -yqq wget make clang-format gcc valgrind python3-pip lcov git
source install_cmake.sh $MODE
