#!/bin/bash

set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/.. && pwd)
cd $ROOT

[[ $NO_DEPS != 1 ]] && ./.circleci/ci_get_deps.sh

mkdir -p build-coverage
cd build-coverage
cmake .. -DCMAKE_BUILD_TYPE=DEBUG \
    -DRS_RUN_TESTS=ON \
    -DUSE_COVERAGE=ON

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi

make -j$CI_CONCURRENCY

cat >rltest.config <<EOF
--unix
EOF
export CONFIG_FILE="$PWD/rltest.config"
export CODE_COVERAGE=1
export RS_GLOBAL_DTORS=1

./lcov-init.sh
COMPAT_DIR=$ROOT/build-coverage make -C $ROOT test CTEST_ARGS="--output-on-failure" CTEST_PARALLEL=${CI_CONCURRENCY}
./lcov-capture.sh coverage.info
bash <(curl -s https://codecov.io/bash) -f coverage.info
lcov -l coverage.info
genhtml --legend -o report coverage.info > /dev/null 2>&1
