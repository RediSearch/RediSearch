#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

$MODE dnf update -y
$MODE dnf install -y gcc gcc-c++ gdb gzip git libstdc++-static make openssl openssl-devel rsync tar unzip wget which xz

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
