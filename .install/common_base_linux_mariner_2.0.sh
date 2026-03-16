#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip rsync \
                         openssl-devel openssl which gzip xz gdb curl

# Install LLVM from the official tarball. The pre-built binary requires a
# newer libstdc++ than Mariner 2.0 ships (GLIBCXX_3.4.30 / GCC 12+), so we
# point LD_LIBRARY_PATH at the bundled libraries.
source "$(dirname "$0")/install_llvm.sh" "$MODE"

LLVM_INSTALL_DIR="${LLVM_INSTALL_DIR:-/usr/local/llvm}"
export LD_LIBRARY_PATH="${LLVM_INSTALL_DIR}/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> "$GITHUB_ENV"
fi
