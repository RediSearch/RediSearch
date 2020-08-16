#!/bin/bash
set -e
set -x

python -m pip install --upgrade pip==19.3.1
pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git
pip install redis-py-cluster

PROJECT_DIR=$PWD
cd $PROJECT_DIR/$BUILD_DIR
ctest -V
