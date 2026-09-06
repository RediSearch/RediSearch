#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# Do NOT install distro `clang`/`libclang-dev` here: on Noble those resolve to
# LLVM 18, which plants /usr/lib/bfd-plugins/LLVMgold.so v18. install_llvm.sh
# below pulls clang-21 + libclang-21-dev from apt.llvm.org (Rust's LLVM major).
# Two LLVMs side-by-side cause `ar` to emit "Unknown attribute kind (102)"
# warnings while indexing LTO-bitcode archives (v18 plugin can't read v21 IR).
apt_install git wget build-essential lcov openssl libssl-dev \
    unzip rsync curl gdb

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    apt_install python3-dev
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
