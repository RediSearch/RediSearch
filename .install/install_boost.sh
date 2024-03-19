#!/bin/bash

set -e
VERSION=$1
MODE=$2
BOOST_DIR="boost_${VERSION//./_}"

wget https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/${BOOST_DIR}.tar.gz

tar -xzf ${BOOST_DIR}.tar.gz
cd ${BOOST_DIR}
echo "BOOST_DIR=$PWD" >> $GITHUB_ENV
echo "export BOOST_DIR = $BOOST_DIR"

./bootstrap.sh --prefix=.
$MODE ./b2 install
