#!/bin/bash

set -e
# set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(realpath $HERE/..)
cd $ROOT

./.circleci/ci_get_deps.sh

mkdir -p build-coverage
cd build-coverage
cmake .. -DCMAKE_BUILD_TYPE=DEBUG \
    -DRS_RUN_TESTS=ON \
    -DUSE_COVERAGE=ON

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi

make -j$CI_CONCURRENCY
BRANCH=master $ROOT/sbin/get-redisjson

cat >rltest.config <<EOF
--unix
EOF
export CONFIG_FILE="$PWD/rltest.config"

./lcov-init.sh
COMPAT_DIR=$ROOT/build-coverage make -C $ROOT test CTEST_ARGS="--output-on-failure" CTEST_PARALLEL=${CI_CONCURRENCY}
# ctest --output-on-failure -j$CI_CONCURRENCY
./lcov-capture.sh coverage.info
bash <(curl -s https://codecov.io/bash) -f coverage.info
lcov -l coverage.info
genhtml --legend -o report coverage.info > /dev/null 2>&1
