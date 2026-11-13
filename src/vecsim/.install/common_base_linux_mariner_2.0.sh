#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE tdnf install -y build-essential git wget ca-certificates which

source install_cmake.sh $MODE
