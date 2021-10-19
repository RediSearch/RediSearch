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
	SAN_MODE=address
    extra_flags="-DUSE_ASAN=ON"
    $READIES/bin/getredis --force -v 6.2 --own-openssl --no-run --suffix asan --clang-asan --clang-san-blacklist /build/redis.blacklist
elif [[ $MSAN == 1 ]]; then
    mode=msan
	SAN_MODE=memory
    extra_flags="-DUSE_MSAN=ON -DMSAN_PREFIX=${SAN_PREFIX}"
    $READIES/bin/getredis --force -v 6.2 --own-openssl --no-run --suffix msan --clang-msan --llvm-dir /opt/llvm-project/build-msan --clang-san-blacklist /build/redis.blacklist
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
    -DVECSIM_ARCH=native \
    $extra_flags \
    ..

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi
if (( $CI_CONCURRENCY > 20 )); then
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
if [[ ! -d RedisJSON ]]; then
	git clone --quiet --recursive https://github.com/RedisJSON/RedisJSON.git
fi

cd RedisJSON
git checkout master
git pull --quiet --recurse-submodules
$READIES/bin/getpy3
./system-setup.py
source /etc/profile.d/rust.sh
make nightly
make SAN="$SAN_MODE"
export REJSON_PATH=$ROOT/deps/RedisJSON/target/x86_64-unknown-linux-gnu/debug/rejson.so

COMPAT_DIR="$ROOT/build-${mode}" make -C $ROOT test SAN="$SAN_MODE" CTEST_ARGS="--output-on-failure" CTEST_PARALLEL="$CI_CONCURRENCY"
