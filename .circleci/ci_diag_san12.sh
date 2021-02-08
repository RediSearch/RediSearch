#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)
cd $ROOT

./.circleci/ci_get_deps.sh

SAN_PREFIX=/opt/llvm-project/build-msan

extra_flags=""

if [[ $ASAN == 1 ]]; then
	mode=asan
    extra_flags="-DUSE_ASAN=ON"
elif [[ $MSAN == 1 ]]; then
	mode=msan
    extra_flags="-DUSE_MSAN=ON -DMSAN_PREFIX=${SAN_PREFIX}"
else
    echo "Should define either ASAN=1 or MSAN=1"
    exit 1
fi

mkdir -p build-${mode}
cd build-${mode}

cmake -DCMAKE_BUILD_TYPE=DEBUG \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DRS_RUN_TESTS=ON \
    $extra_flags \
    ..

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi
if [[ $CI_CONCURRENCY > 20 ]]; then
	CI_CONCURRENCY=20
fi

make -j$CI_CONCURRENCY

cat >rltest.config <<EOF
--oss-redis-path=redis-server-${mode}
--no-output-catch
--exit-on-failure
--check-exitcode
--unix
EOF

export ASAN_OPTIONS=detect_odr_violation=0
export RS_GLOBAL_DTORS=1

ctest --output-on-failure -j$CI_CONCURRENCY
