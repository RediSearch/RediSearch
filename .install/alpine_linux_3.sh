#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE apk update

$MODE apk add --no-cache build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang18 clang18-libclang curl \
    clang18-static ncurses-dev llvm18-dev bash

# Create symlinks for clang and llvm-config (Alpine uses versioned names)
if [ ! -e /usr/bin/clang ] && [ -e /usr/bin/clang-18 ]; then
    $MODE ln -sf /usr/bin/clang-18 /usr/bin/clang
    $MODE ln -sf /usr/bin/clang++-18 /usr/bin/clang++
fi
if [ ! -e /usr/bin/llvm-config ]; then
    $MODE ln -sf /usr/bin/llvm18-config /usr/bin/llvm-config
fi

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
