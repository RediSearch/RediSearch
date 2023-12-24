#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y
$MODE dnf install -y gcc-toolset-10-gcc gcc-toolset-10-gcc-c++ make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which

source /opt/rh/gcc-toolset-10/enable
source install_cmake.sh $MODE

# Install python3.10 (latest version suggested by the default package manager is python3.6)
$MODE wget https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tar.xz
$MODE tar -xf Python-3.10.0.tar.xz
cd Python-3.10.0
$MODE ./configure --enable-optimizations
$MODE make -j 2
$MODE make altinstall
