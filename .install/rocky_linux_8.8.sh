#!/bin/bash
set -e
yum install -y gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ make valgrind wget git
source /opt/rh/gcc-toolset-10/enable
source install_cmake.sh

