#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ make wget git --nobest --skip-broken --allowerasing


cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
# install other stuff after installing gcc-toolset-13 to avoid dependencies conflicts
$MODE dnf install -y openssl openssl-devel which rsync unzip curl clang  clang-devel --nobest --skip-broken --allowerasing
