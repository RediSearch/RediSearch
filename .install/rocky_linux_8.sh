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

# lld: EL8's LLVM stream ships clang as clang-<major> (e.g. clang-21), and
# cross-language LTO links with '-fuse-ld=lld' — without the lld package
# clang fails with "invalid linker name in argument '-fuse-ld=lld'".
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

# Source for _this_ shell only, so the rest of the bootstrap builds with it.
# Not copied to /etc/profile.d and no /usr/local/bin symlinks: those would
# silently change the default compiler for every other checkout on this
# host. build.sh enables the toolset itself when CC is unset.
source /opt/rh/gcc-toolset-13/enable
