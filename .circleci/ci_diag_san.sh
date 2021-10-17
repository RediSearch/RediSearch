#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)
cd $ROOT

./.circleci/ci_get_deps.sh

SAN_PREFIX=/opt/san

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

## Add some configuration options to our rltest file

export REDIS_SERVER=${SAN_PREFIX}/bin/redis-server-${mode}
cat >rltest.config <<EOF
--no-output-catch
--exit-on-failure
--check-exitcode
--unix
EOF
export CONFIG_FILE="$PWD/rltest.config"

export ASAN_OPTIONS=detect_odr_violation=0
export RS_GLOBAL_DTORS=1

# FIXME: Need to change the image once this actually works..
ln -sf /usr/bin/llvm-symbolizer-4.0 /usr/bin/llvm-symbolizer || true

# COMPAT_DIR="$ROOT/build-${mode}" make -C $ROOT test CTEST_ARGS="--output-on-failure -j$CI_CONCURRENCY"
ctest --output-on-failure -j$CI_CONCURRENCY
