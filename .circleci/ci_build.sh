#!/bin/bash
set -e
set -x

if [ -z "$CI_CONCURRENCY" ];
then
    CI_CONCURRENCY=8
fi

PROJECT_DIR=$PWD
mkdir $BUILD_DIR
cd $BUILD_DIR

cmake $PROJECT_DIR \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRS_RUN_TESTS=1 \
    -RS_VERBOSE_TESTS=1 ${extra_args} \
    ../
make -j$CI_CONCURRENCY
