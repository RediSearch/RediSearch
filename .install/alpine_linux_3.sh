#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE apk update

$MODE apk add --no-cache build-base gcc g++ make linux-headers openblas-dev \
    xsimd curl wget git python3 python3-dev py3-pip openssl openssl-dev \
    tar xz which rsync bsd-compat-headers clang clang17-libclang lld curl llvm clang-dev \
    bash cargo gcc autoconf automake py3-cryptography \
    musl-dev libffi-dev openssh py-virtualenv clang18-libclang gcompat

source install_cmake.sh $MODE
