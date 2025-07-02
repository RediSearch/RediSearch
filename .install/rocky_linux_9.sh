#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git \
    openssl openssl-devel python3 python3-devel which rsync unzip clang curl --nobest --skip-broken --allowerasing

cp /opt/rh/gcc-toolset-14/enable /etc/profile.d/gcc-toolset-14.sh
