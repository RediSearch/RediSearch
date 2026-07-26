#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# Full system upgrade — real-only (not a dep; `dnf install` doesn't need it, so
# list/dry-run stay clean). Runs on a real bootstrap.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then $MODE dnf update -y; fi
dnf_install gcc gcc-c++ gdb gzip git libstdc++-static make openssl openssl-devel rsync tar unzip wget which xz

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
