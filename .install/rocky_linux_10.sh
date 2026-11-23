#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
$MODE dnf update -y

# Rocky 10 ships GCC 14 natively — no need for gcc-toolset
$MODE dnf install -y gcc gcc-c++ make wget git diffutils --nobest --skip-broken --allowerasing

# python3-devel: the test requirements build C extensions (psutil) against the system Python
$MODE dnf install -y openssl openssl-devel python3-devel which rsync unzip curl clang clang-devel gdb xz --nobest --skip-broken --allowerasing
