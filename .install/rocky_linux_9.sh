#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y
$MODE dnf install -y gcc gcc-c++ make wget git openssl openssl-devel
source install_cmake.sh $MODE

echo INSTALLER="dnf" >> $GITHUB_ENV
