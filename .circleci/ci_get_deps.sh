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
wget -q https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
python /tmp/get-pip.py
python -m pip install --upgrade setuptools
python -m pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest rmtest semantic-version
python -m pip install --no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master
python -m pip install --no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master
python -m pip install --no-cache-dir git+https://github.com/RedisLabs/RAMP@master
