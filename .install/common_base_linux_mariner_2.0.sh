#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip rsync \
                         openssl-devel openssl python3 python3-pip which

source install_cmake.sh $MODE
