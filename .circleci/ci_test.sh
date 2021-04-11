#!/bin/bash

set -e
# set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(realpath $HERE/..)

cd $ROOT/$BUILD_DIR

if [[ ! -z $USE_COVERAGE ]]; then
    ./lcov-init.sh
fi

COMPAT_DIR="$ROOT/$BUILD_DIR" make -C $ROOT test CTEST_ARGS="-V"

if [[ ! -z $USE_COVERAGE ]]; then
    ./lcov-capture.sh $ROOT/coverage
fi
