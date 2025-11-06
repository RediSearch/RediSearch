#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE dnf update -y

# Development Tools includes config-manager
$MODE dnf groupinstall "Development Tools" -yqq

# powertools is needed to install epel
$MODE dnf config-manager --set-enabled powertools

# get epel to install gcc13
$MODE dnf install epel-release -yqq

$MODE dnf install -y gcc gcc-c++ \
    gcc-toolset-13-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync \
    clang curl clang-devel --nobest --skip-broken

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE dnf install -y python3.12-devel
fi

cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
