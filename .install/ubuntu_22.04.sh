#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -yqq git wget build-essential valgrind lcov
source install_cmake.sh
