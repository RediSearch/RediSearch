#!/bin/sh
set -x
if [ -z "$CC" ]; then
    CC=clang
fi
if [ -z "$CXX" ]; then
    CXX=clang++
fi

cmake \
    -DRS_RUN_TESTS=ON \
    -DCMAKE_BUILD_TYPE=DEBUG \
    -DUSE_MSAN=ON \
    -DCMAKE_C_COMPILER="$CC" \
    -DCMAKE_CXX_COMPILER="$CXX" \
    -DCMAKE_LINKER=clang $@
