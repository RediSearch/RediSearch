#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf groupinstall "Development Tools" -yqq

$MODE dnf install -y gcc-toolset-12-gcc gcc-toolset-12-gcc-c++ make wget git openssl openssl-devel python3 which

cp /opt/rh/gcc-toolset-12/enable /etc/profile.d/gcc-toolset-12.sh
bash

source install_cmake.sh $MODE
