#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf groupinstall "Development Tools" -yqq

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ make wget git openssl openssl-devel python3 which

cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
bash

source install_cmake.sh $MODE
