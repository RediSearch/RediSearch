#!/bin/bash

set -e
VERSION=$1
MODE=$2 # whether to install using sudo or not

BOOST_DIR="boost_${VERSION//./_}"

wget https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/${BOOST_DIR}.tar.gz

tar -xzf ${BOOST_DIR}.tar.gz
cd ${BOOST_DIR}

./bootstrap.sh --prefix=/usr
$MODE ./b2 install
