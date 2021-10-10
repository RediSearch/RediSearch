#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT="$(cd $HERE/.. && pwd)"
READIES="$ROOT/deps/readies"
cd $ROOT

./.circleci/ci_get_deps.sh

if [[ $ASAN == 1 ]]; then
	JSON_SAN_MODE=address
elif [[ $MSAN == 1 ]]; then
	JSON_SAN_MODE=memory
else
    echo "Should define either ASAN=1 or MSAN=1"
    exit 1
fi

make build SAN=${SAN_MODE}

cd $ROOT/deps
if [[ ! -d RedisJSON ]]; then
	git clone --quiet --recursive https://github.com/RedisJSON/RedisJSON.git
endif

cd RedisJSON
git checkout master
git pull --quiet --recurse-submodules
$READIES/bin/getpy3
./system-setup.py
source /etc/profile.d/rust.sh
make nightly
make SAN=$JSON_SAN_MODE

cd $ROOT
make pytest SAN=${SAN_MODE}
