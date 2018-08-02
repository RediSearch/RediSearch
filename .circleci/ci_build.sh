#!/bin/bash
set -e
set -x

PROJECT_DIR=$PWD
mkdir $BUILD_DIR
cd $BUILD_DIR
cmake $PROJECT_DIR -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j8