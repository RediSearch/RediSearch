#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y
$MODE dnf install -y gcc gcc-c++ gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which

source /opt/rh/gcc-toolset-10/enable
source install_cmake.sh $MODE

# Install python3.10 (latest version suggested by the default package manager is python3.6)
$MODE dnf install python39 -y
$MODE alternatives --set python `which python3.9`
update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1
