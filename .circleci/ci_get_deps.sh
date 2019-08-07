#!/bin/sh
set -x
./srcutil/get_gtest.sh
pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git
pip install redis-py-cluster
