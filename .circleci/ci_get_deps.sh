#!/bin/bash

set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)
cd $ROOT

git submodule update --init --recursive
PIP=1 FORCE=1 ./deps/readies/bin/getpy2
./system-setup.py
./srcutil/get_gtest.sh
python ./src/pytest/test_rdb_compatibility.py
