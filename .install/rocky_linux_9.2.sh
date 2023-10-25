#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE dnf update
$MODE dnf install -y gcc gcc-c++ make wget git
source install_cmake.sh $MODE
