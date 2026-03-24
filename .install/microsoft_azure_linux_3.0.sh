#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive

$MODE tdnf install -yq build-essential ca-certificates gdb git libxcrypt-devel openssl-devel rsync tar unzip wget which xz

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE tdnf install -y python3-devel
fi
