#!/bin/sh
set -e
set -x

if [ -e '/etc/redhat-release' ]; then
    yum remove python-setuptools || true
    pip uninstall -y setuptools || true
fi


git submodule update --init --recursive
./srcutil/get_gtest.sh
python ./src/pytest/test_rdb_compatibility.py

pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git
pip install redis-py-cluster
