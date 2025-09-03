#!/bin/bash

set -e
VERSION=1.84.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
    exit 0
fi

wget https://github.com/boostorg/boost/releases/download/boost-${VERSION}/boost-${VERSION}.tar.gz -O ${BOOST_NAME}.tar.gz

tar -xzf ${BOOST_NAME}.tar.gz
cd boost-${VERSION}
echo "Building Boost headers..."
./bootstrap.sh --with-libraries=headers
./b2 headers
cd ..
mv boost-${VERSION} ${BOOST_DIR}
rm ${BOOST_NAME}.tar.gz
