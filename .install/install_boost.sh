#!/usr/bin/env bash

set -e
VERSION=1.84.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
    exit 0
fi

wget https://sourceforge.net/projects/boost/files/boost/${VERSION}/${BOOST_NAME}.tar.gz/download -O ${BOOST_NAME}.tar.gz

tar -xzf ${BOOST_NAME}.tar.gz
mv ${BOOST_NAME} ${BOOST_DIR}
rm ${BOOST_NAME}.tar.gz
