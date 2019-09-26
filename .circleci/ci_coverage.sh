#!/bin/bash
set -e
set -x

if [ -z "$CI_CONCURRENCY" ];
then
    CI_CONCURRENCY=8
fi

./.circleci/ci_get_deps.sh
apt-get update
apt-get -y install lcov curl

mkdir build-coverage
cd build-coverage
cmake .. -DCMAKE_BUILD_TYPE=DEBUG \
    -DRS_RUN_TESTS=ON \
    -DUSE_COVERAGE=ON

make -j20

cat >rltest.config <<EOF
--unix
EOF

./lcov-init.sh
ctest
./lcov-capture.sh coverage.info
bash <(curl -s https://codecov.io/bash) -f coverage.info
