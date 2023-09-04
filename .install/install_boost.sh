#!/bin/bash

set -e
VERSION=$1
UNDERSCORE_VERSION=${VERSION//./_}

wget https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/boost_${VERSION//./_}.tar.gz -O boost.tar.gz

tar -xzf boost.tar.gz
cd boost_${VERSION//./_}

./bootstrap.sh --prefix=/usr/
./b2 install
