#!/bin/sh
set -x
git submodule update --init --recursive
./srcutil/get_gtest.sh
python ./src/pytest/test_rdb_compatibility.py

pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git
pip install redis-py-cluster
