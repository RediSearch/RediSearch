#!/bin/bash
set -e
set -x

PROJECT_DIR=$PWD
cd $PROJECT_DIR/$BUILD_DIR

if [ "$USE_COVERAGE" ]; then
    ./lcov-init.sh
fi

ctest -V
if [ "$USE_COVERAGE" ]; then
    sudo apt-get install lcov
    ./lcov-capture.sh ${PROJECT_DIR}/coverage
fi
