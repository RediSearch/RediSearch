#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip \
                         rsync openssl-devel \
                         which clang libxcrypt-devel clang-devel

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE tdnf install -y python3-devel
fi
