#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE yum install -y "Development Tools" wget git
source install_cmake.sh $MODE
