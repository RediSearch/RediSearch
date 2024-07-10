#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE apk update

$MODE apk add --no-cache gcc g++ make openblas-dev xsimd curl wget git \
    python3 python3-dev py3-pip openssl openssl-dev tar xz which rsync clang

$MODE apk add cmake --no-cache
#source install_cmake.sh $MODE