#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT="$(cd $HERE/.. && pwd)"
READIES="$ROOT/deps/readies"
cd $ROOT

./.circleci/ci_get_deps.sh

SAN_PREFIX=/opt/llvm-project/build-msan

extra_flags=""

echo "fun:THPIsEnabled" >> /build/redis.blacklist
if [[ $ASAN == 1 ]]; then
    mode=asan
	JSON_SAN_MODE=address
    extra_flags="-DUSE_ASAN=ON"
    $READIES/bin/getredis --force -v 6.2 --no-tls --no-run --suffix asan --clang-asan --clang-san-blacklist /build/redis.blacklist
elif [[ $MSAN == 1 ]]; then
    mode=msan
	JSON_SAN_MODE=memory
    extra_flags="-DUSE_MSAN=ON -DMSAN_PREFIX=${SAN_PREFIX}"
    $READIES/bin/getredis --force -v 6.2 --no-tls --no-run --suffix msan --clang-msan --llvm-dir /opt/llvm-project/build-msan --clang-san-blacklist /build/redis.blacklist
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

export REDIS_SERVER=redis-server-${mode}
cat >rltest.config <<EOF
--no-output-catch
--exit-on-failure
--check-exitcode
--unix
EOF
export CONFIG_FILE="$PWD/rltest.config"

export ASAN_OPTIONS=detect_odr_violation=0
export RS_GLOBAL_DTORS=1

cd $ROOT/deps
[[ ! -d RedisJSON ]] && git clone --recursive https://github.com/RedisJSON/RedisJSON.git
cd RedisJSON
git checkout master
git pull --recurse-submodules
# ./deps/readies/bin/getpy3
$READIES/bin/getpy3
./system-setup.py
source /etc/profile.d/rust.sh
make nightly
make SAN=$JSON_SAN_MODE
export REJSON_PATH=$ROOT/deps/RedisJSON/target/x86_64-unknown-linux-gnu/debug/rejson.so
export SANITIZER=1
export SHORT_READ_BYTES_DELTA=512

COMPAT_DIR="$ROOT/build-${mode}" make -C $ROOT test CTEST_ARGS="--output-on-failure" CTEST_PARALLEL="$CI_CONCURRENCY"
