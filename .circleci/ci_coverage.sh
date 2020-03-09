#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd $HERE/..

[[ -z $CI_CONCURRENCY ]] && CI_CONCURRENCY=$(./deps/readies/bin/nproc)

./.circleci/ci_get_deps.sh

mkdir -p build-coverage
cd build-coverage
cmake .. -DCMAKE_BUILD_TYPE=DEBUG \
    -DRS_RUN_TESTS=ON \
    -DUSE_COVERAGE=ON

make -j20

cat >rltest.config <<EOF
--unix
EOF

./lcov-init.sh
ctest -j20
./lcov-capture.sh coverage.info
bash <(curl -s https://codecov.io/bash) -f coverage.info
lcov -l coverage.info
genhtml --legend -o report coverage.info > /dev/null 2>&1
