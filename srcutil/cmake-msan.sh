#!/bin/sh
set -x
if [ -n "$CLANG_VERSION" ]
then
    CLANG_CXX="clang++-${CLANG_VERSION}"
    CLANG_C="clang-${CLANG_VERSION}"
else
    CLANG_C=clang
    CLANG_CXX=clang++
fi

cmake \
    -DRS_RUN_TESTS=ON \
    -DCMAKE_BUILD_TYPE=DEBUG \
    -DUSE_MSAN=ON \
    -DCMAKE_C_COMPILER="$CLANG_C" \
    -DCMAKE_CXX_COMPILER="$CLANG_CXX" \
    -DCMAKE_LINKER=clang $@
