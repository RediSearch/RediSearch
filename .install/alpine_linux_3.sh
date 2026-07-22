#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# apk index refresh — real-only (not a dep; `apk add --no-cache` works without
# it, so list/dry-run stay clean). Runs on a real bootstrap.
if [[ "${CHECK_DEPS:-0}" != 1 && "${DRY_RUN:-0}" != 1 ]]; then $MODE apk update; fi

apk_install build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang curl \
    clang-static ncurses-dev llvm-dev compiler-rt bash

# We must install Python via the package manager until
# `uv` starts providing aarch64-musl builds.
# See https://github.com/astral-sh/python-build-standalone/pull/569
if [[ "$(uname -m)" == "aarch64" ]]; then
    apk_install python3 python3-dev py3-pip
    # Needed before checkout
    apk_install gcompat libstdc++ libgcc
else
    # On x86_64, we need Python headers to build psutil@5.x.y from
    # source, since it only started providing wheels for musl
    # in version 6.w.z.
    apk_install python3-dev
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
