#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE yum install -y gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ make valgrind wget git
$MODE source /opt/rh/gcc-toolset-10/enable
$MODE source install_cmake.sh
