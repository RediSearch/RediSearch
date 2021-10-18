#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT="$(cd $HERE/.. && pwd)"
READIES="$ROOT/deps/readies"
cd $ROOT

./.circleci/ci_get_deps.sh

$READIES/bin/getredis -v 6 --valgrind --suffix vg
$READIES/bin/getvalgrind

if [[ -z $CI_CONCURRENCY ]]; then
	CI_CONCURRENCY=$($ROOT/deps/readies/bin/nproc)
fi
if [[ $CI_CONCURRENCY > 20 ]]; then
	CI_CONCURRENCY=20
fi

make VG=1 -j$CI_CONCURRENCY

export REDIS_SERVER=redis-server-vg
cat >rltest-vg.config <<EOF
--no-output-catch
--exit-on-failure
--check-exitcode
--unix
EOF
export CONFIG_FILE="$ROOT/rltest-vg.config"

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
# make nightly
make DEBUG=1

make -C $ROOT pytest \
	VG=1 VG_LEAKS=0 \
	REJSON_PATH=$ROOT/deps/RedisJSON/target/debug/rejson.so \
	CTEST_ARGS="--output-on-failure" CTEST_PARALLEL="$CI_CONCURRENCY"
