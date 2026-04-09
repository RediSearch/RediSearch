#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
$MODE dnf update -y

# Rocky 10 ships GCC 14 natively — no need for gcc-toolset
$MODE dnf install -y gcc gcc-c++ make wget git --nobest --skip-broken --allowerasing

$MODE dnf install -y openssl openssl-devel python3-devel which rsync unzip curl gdb xz --nobest --skip-broken --allowerasing

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
