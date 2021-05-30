#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT="$(cd $HERE/.. && pwd)"
READIES="$ROOT/deps/readies"
cd $ROOT

./.circleci/ci_get_deps.sh

SAN_PREFIX=/opt/llvm-project/build-msan

if [[ $ASAN == 1 ]]; then
	SAN_MODE=address
elif [[ $MSAN == 1 ]]; then
	SAN_MODE=memory
else
    echo "Should define either ASAN=1 or MSAN=1"
    exit 1
fi

make build SAN=${SAN_MODE}

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi
if [[ $CI_CONCURRENCY > 20 ]]; then
	CI_CONCURRENCY=20
fi

cd $ROOT/deps
git clone --recursive https://github.com/RedisJSON/RedisJSON.git
cd RedisJSON
git checkout master
./deps/readies/bin/getpy3
./system-setup.py
source /etc/profile.d/rust.sh
make nightly
make SAN=${SAN_MODE}
export REJSON_PATH=$ROOT/deps/RedisJSON/target/x86_64-unknown-linux-gnu/debug/rejson.so

cd $ROOT
make test SAN=${SAN_MODE} CTEST_ARGS="--output-on-failure" CTEST_PARALLEL=${CI_CONCURRENCY}
