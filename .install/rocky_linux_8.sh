#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

$MODE dnf update -y

# Development Tools includes config-manager
$MODE dnf groupinstall "Development Tools" -yqq

# powertools (Rocky/Alma) or codeready-builder (RHEL) is needed to install epel
$MODE dnf config-manager --set-enabled powertools 2>/dev/null || \
    $MODE dnf config-manager --set-enabled "codeready-builder-for-rhel-8-$(uname -m)-rpms" 2>/dev/null || true

# get epel to install gcc13
$MODE dnf install epel-release -yqq

$MODE dnf install -y gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ \
    gcc-toolset-13-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync \
    clang curl clang-devel lld gdb --nobest --skip-broken

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE dnf install -y python3.12-devel
fi

$MODE cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
$MODE ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/gcc  /usr/local/bin/gcc  || true
$MODE ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/g++  /usr/local/bin/g++  || true
$MODE ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/cc   /usr/local/bin/cc   || true
$MODE ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/as   /usr/local/bin/as   || true
$MODE ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/make /usr/local/bin/make || true
