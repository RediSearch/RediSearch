#!/bin/bash
set -e
set -x

pip install wheel
pip install setuptools --upgrade
pip install git+https://github.com/RedisLabsModules/RLTest.git
pip install redis-py-cluster==1.3.6

PROJECT_DIR=$PWD
cd $PROJECT_DIR/$BUILD_DIR
ctest -V
