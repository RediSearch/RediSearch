#!/bin/bash
set -e
set -x

PROJECT_DIR=$PWD
cd $PROJECT_DIR/$BUILD_DIR
ctest -V
