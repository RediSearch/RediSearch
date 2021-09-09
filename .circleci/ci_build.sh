#!/bin/bash

set -e
echo $PATH
export PATH/usr/local/bin:$PATH

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)
cd $ROOT

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi

mkdir -p $BUILD_DIR
COMPAT_DIR=$ROOT/$BUILD_DIR make -C $ROOT WITH_TESTS=1 -j$CI_CONCURRENCY
