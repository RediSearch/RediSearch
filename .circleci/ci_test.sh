#!/bin/bash
set -e
set -x
pip install git+https://github.com/RedisLabs/rmtest@2.0
cd $BUILD_DIR
ctest -V -j8