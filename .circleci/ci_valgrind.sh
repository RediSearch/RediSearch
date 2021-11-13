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
if (( $CI_CONCURRENCY > 20 )); then
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


make -C $ROOT pytest \
	VG=1 VG_LEAKS=0 \
	CTEST_ARGS="--output-on-failure" CTEST_PARALLEL="$CI_CONCURRENCY"
