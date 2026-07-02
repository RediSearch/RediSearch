#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

$MODE apk update

$MODE apk add --no-cache build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang curl \
    clang-static ncurses-dev llvm-dev bash

# We must install Python via the package manager until
# `uv` starts providing aarch64-musl builds.
# See https://github.com/astral-sh/python-build-standalone/pull/569
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE apk add --no-cache python3 python3-dev py3-pip
    # Needed before checkout
    $MODE apk add --no-cache gcompat libstdc++ libgcc
else
    # On x86_64, we need Python headers to build psutil@5.x.y from
    # source, since it only started providing wheels for musl
    # in version 6.w.z.
    $MODE apk add --no-cache python3-dev
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# Static LLVM/clang libraries for bindgen-static mode (redis-module musl target
# in document_metadata uses bindgen-static, which links clang-sys statically).
LLVM_VER=$(ls /usr/lib/ | grep -oE 'llvm[0-9]+' | sort -V | tail -1 | tr -d 'llvm')
$MODE apk add --no-cache llvm${LLVM_VER}-static ncurses-static zlib-static zstd-static

# Alpine ships component .a files but no combined libLLVM-<ver>.a.
# clang-sys emits cargo:rustc-link-lib=LLVM-<ver> which the linker resolves to
# libLLVM-<ver>.a. Create a thin archive that references the component files.
if [ ! -e /usr/lib/llvm${LLVM_VER}/lib/libLLVM-${LLVM_VER}.a ]; then
    # shellcheck disable=SC2046
    ar rcT /usr/lib/llvm${LLVM_VER}/lib/libLLVM-${LLVM_VER}.a \
        /usr/lib/llvm${LLVM_VER}/lib/libLLVM*.a \
        /usr/lib/libzstd.a
fi
