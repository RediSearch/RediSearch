#!/bin/bash

set -e
VERSION=1.84.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
    exit 0
fi

BOOST_URL=https://github.com/boostorg/boost/releases/download/boost-${VERSION}/boost-${VERSION}.tar.gz
if [[ "$(uname -s)" == Darwin ]]; then
    # Homebrew's wget on the macOS x86_64 runners can't verify github.com's
    # certificate chain; curl uses the system trust store.
    curl -fsSL --retry 3 "${BOOST_URL}" -o ${BOOST_NAME}.tar.gz
else
    wget "${BOOST_URL}" -O ${BOOST_NAME}.tar.gz
fi

tar -xzf ${BOOST_NAME}.tar.gz
cd boost-${VERSION}
echo "Building Boost headers..."
./bootstrap.sh --with-libraries=headers
./b2 headers
cd ..
mv boost-${VERSION} ${BOOST_DIR}
rm ${BOOST_NAME}.tar.gz
