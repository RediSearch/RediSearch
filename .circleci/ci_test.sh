#!/bin/bash

set -e
# set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)

BUILD_DIR=${BUILD_DIR:-build}
cd $ROOT/$BUILD_DIR

if [[ ! -z $USE_COVERAGE ]]; then
    ./lcov-init.sh
fi

BRANCH=master $ROOT/sbin/get-redisjson
COMPAT_DIR="$ROOT/$BUILD_DIR" make -C $ROOT test CTEST_ARGS="-V"

if [[ ! -z $USE_COVERAGE ]]; then
    ./lcov-capture.sh $ROOT/coverage
fi
