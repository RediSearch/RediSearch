#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE apk update

$MODE apk add --no-cache build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang clang17-libclang curl \
    clang-static ncurses-dev llvm-dev

# We must install Python via the package manager until
# `uv` starts providing aarch64-musl builds.
# See https://github.com/astral-sh/python-build-standalone/pull/569
if [ "$(uname -m)" = "aarch64" ]; then
    $MODE apk add --no-cache python3 python3-dev py3-pip
else
    # On x86_64, we need Python headers to build psutil@5.x.y from
    # source, since it only started providing wheels for musl
    # in version 6.w.z.
    $MODE apk add --no-cache python3-dev
fi
