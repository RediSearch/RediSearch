#!/bin/bash

set -e
# set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)
cd $ROOT

PROJECT_DIR=$PWD
mkdir -p $BUILD_DIR
cd $BUILD_DIR

cmake $PROJECT_DIR \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRS_RUN_TESTS=1 \
    -DRS_VERBOSE_TESTS=$(VERBOSE) \
    ../

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi

COMPAT_DIR=$ROOT/$BUILD_DIR make -C $ROOT -j$CI_CONCURRENCY
