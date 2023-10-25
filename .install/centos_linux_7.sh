#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE yum install -y "Development Tools" wget git
source /opt/rh/gcc-toolset-10/enable
source install_cmake.sh $MODE
