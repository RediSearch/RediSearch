#!/usr/bin/env bash
set -eo pipefail
MODE=$1 # whether to install using sudo or not

# Arch's own clang is used as-is; install_llvm.sh only matters for LTO, which
# this platform does not build with. Arch ships one openssl package with headers.
pacman_install base-devel gcc make git wget openssl \
    unzip rsync clang curl gdb cmake python
