#!/bin/sh
set -x
./srcutil/get_gtest.sh
python ./src/pytest/test_rdb_compatibility.py

pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git@check-before-flush
pip install redis-py-cluster
