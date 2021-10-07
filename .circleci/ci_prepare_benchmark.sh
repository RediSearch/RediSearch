#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT="$(cd $HERE/.. && pwd)"
READIES="$ROOT/deps/readies"
cd $ROOT
ENABLE_SYSTEM_SETUP=${ENABLE_SYSTEM_SETUP:-"0"}

if [ "${ENABLE_SYSTEM_SETUP}" = "1" ]; then
    ./.circleci/ci_get_deps.sh
fi
# 

cd $ROOT/deps
rm -rf $ROOT/deps/RedisJSON
git clone --recursive https://github.com/RedisJSON/RedisJSON.git
cd RedisJSON
git checkout master
if [ "${ENABLE_SYSTEM_SETUP}" = "1" ]; then
    ./deps/readies/bin/getpy3
    ./system-setup.py
    source /etc/profile.d/rust.sh
fi

make
cd $ROOT
