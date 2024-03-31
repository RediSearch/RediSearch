#!/bin/bash

set -e
VERSION=$1
MODE=$2
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

wget https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/${BOOST_NAME}.tar.gz

tar -xzf ${BOOST_NAME}.tar.gz
mv ${BOOST_NAME} ${BOOST_DIR}
