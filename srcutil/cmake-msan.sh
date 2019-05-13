#!/bin/sh
CLANG=clang
cmake \
    -DCMAKE_BUILD_TYPE=DEBUG \
    -DUSE_MSAN=ON \
    -DCMAKE_C_COMPILER="$CLANG" \
    -DCMAKE_CXX_COMPILER="${CLANG}++" \
    -DCMAKE_LINKER=clang $@
