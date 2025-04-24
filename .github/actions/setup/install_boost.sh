#!/bin/bash

set -e
VERSION=1.84.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
    exit 0
fi

wget https://archives.boost.io/release/${VERSION}/source/${BOOST_NAME}.tar.gz -O ${BOOST_NAME}.tar.gz

tar -xzf ${BOOST_NAME}.tar.gz
mv ${BOOST_NAME} ${BOOST_DIR}
rm ${BOOST_NAME}.tar.gz
