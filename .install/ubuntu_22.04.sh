#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt update -qq
apt install -yqq git wget build-essential lcov clang
source install_cmake.sh
