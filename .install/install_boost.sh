#!/bin/bash

set -e
VERSION=$1
MODE=$2 # whether to install using sudo or not

BOOST_DIR="boost_${VERSION//./_}"

# Download and extract boost if not found (cached)
if [[ ! -d ${BOOST_DIR} ]]; then
    wget https://boostorg.jfrog.io/artifactory/main/release/${VERSION}/source/${BOOST_DIR}.tar.gz
    tar -xzf ${BOOST_DIR}.tar.gz
fi

cd ${BOOST_DIR}

# Build and install boost. Should be fast if cached
./bootstrap.sh --prefix=/usr
$MODE ./b2 install
