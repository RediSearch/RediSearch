#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd $HERE/..

[[ -z $CI_CONCURRENCY ]] && CI_CONCURRENCY=$(./deps/readies/bin/nproc)

PROJECT_DIR=$PWD
mkdir -p $BUILD_DIR
cd $BUILD_DIR

cmake $PROJECT_DIR \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRS_RUN_TESTS=1 \
    -RS_VERBOSE_TESTS=1 ${extra_args} \
    ../
make -j$CI_CONCURRENCY
