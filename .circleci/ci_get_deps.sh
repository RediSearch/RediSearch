#!/bin/bash

set -e
set -x

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ROOT=$(cd $HERE/..; pwd)
cd $ROOT

git submodule update --init --recursive
./deps/readies/bin/getpy2
if [[ $GCC != 0 ]]; then
	python2 ./system-setup.py
else
	python2 ./system-setup.py --no-gcc
fi
python2 ./tests/pytests/test_rdb_compatibility.py
bash -l -c "make fetch"
