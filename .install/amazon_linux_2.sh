#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

$MODE yum update -y
$MODE amazon-linux-extras install epel -y
$MODE yum install -y epel-release yum-utils
# CentOS 7 SCL repo for devtoolset-11 (AL2 ships gcc 7.3; build needs gcc >=10)
$MODE yum-config-manager --add-repo http://vault.centos.org/centos/7/sclo/x86_64/rh/
$MODE yum install -y --nogpgcheck --skip-broken \
    autogen centos-release-scl scl-utils cmake3 \
    devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-make || true
$MODE cp /opt/rh/devtoolset-11/enable /etc/profile.d/devtoolset-11.sh 2>/dev/null || true
source /opt/rh/devtoolset-11/enable 2>/dev/null || true
$MODE ln -sf "$(command -v cmake3)" /usr/bin/cmake
$MODE ln -sf /opt/rh/devtoolset-11/root/usr/bin/make /usr/local/bin/make
$MODE ln -sf /opt/rh/devtoolset-11/root/usr/bin/gcc /usr/local/bin/gcc
$MODE ln -sf /opt/rh/devtoolset-11/root/usr/bin/g++ /usr/local/bin/g++
$MODE ln -sf /opt/rh/devtoolset-11/root/usr/bin/cc  /usr/local/bin/cc
$MODE ln -sf /opt/rh/devtoolset-11/root/usr/bin/as  /usr/local/bin/as  || true

# openssl-devel (1.0.2k) conflicts with openssl11-devel on AL2 — remove it first if present and openssl11-devel is not yet installed
! rpm -q openssl11-devel &>/dev/null && rpm -q openssl-devel &>/dev/null && $MODE yum remove -y openssl-devel || true
$MODE yum install -y gcc gcc-c++ gdb gzip git \
    openssl11 openssl11-devel \
    libstdc++-static make rsync tar unzip wget which xz

# Install LLVM for LTO (Rust bindgen)
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
