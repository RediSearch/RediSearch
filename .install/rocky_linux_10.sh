#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE dnf update -y

# Rocky 10 ships GCC 14 natively — no need for gcc-toolset
$MODE dnf install -y gcc gcc-c++ make wget git diffutils --nobest --skip-broken --allowerasing

# python3-devel: the test requirements build C extensions (psutil) against the system Python
$MODE dnf install -y openssl openssl-devel python3-devel which rsync unzip curl clang clang-devel gdb xz --nobest --skip-broken --allowerasing

# Rocky 9 got these from gcc-toolset; with the system GCC they are separate packages.
# The module links with -static-libstdc++ (libstdc++-static lives in CRB), and Boost
# multiprecision includes quadmath.h where __float128 exists (x86_64 only).
$MODE dnf install -y dnf-plugins-core
$MODE dnf config-manager --set-enabled crb
$MODE dnf install -y libstdc++-static
if [[ $(uname -m) == x86_64 ]]; then
    $MODE dnf install -y libquadmath-devel
fi
