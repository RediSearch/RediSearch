#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt update -qq
apt install -yqq wget make clang-format gcc valgrind python3-pip lcov git
source install_cmake.sh
