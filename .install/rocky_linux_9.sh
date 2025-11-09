#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ make wget git \
    openssl openssl-devel which rsync unzip clang curl clang-devel --nobest --skip-broken --allowerasing

# Remove any gcc-toolset versions other than 13 (keep clang and clang-devel for bindgen)
$MODE dnf remove -y gcc-toolset-* --exclude=gcc-toolset-13-\* 2>/dev/null || true

cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
