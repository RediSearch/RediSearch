#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -yqq wget make clang-format gcc valgrind python3-pip lcov git
source install_cmake.sh
