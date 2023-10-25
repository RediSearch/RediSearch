#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt-get update -qq
$MODE apt-get install -yqq git wget build-essential valgrind lcov
source install_cmake.sh $MODE
