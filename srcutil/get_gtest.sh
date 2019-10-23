#!/bin/bash

set -e

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# GTEST_URL_BASE=https://s3-eu-central-1.amazonaws.com/redislabs-dev-public-deps
GTEST_URL_BASE=https://github.com/google/googletest/archive/
GTEST_FILENAME=release-1.8.0.tar.gz
GTEST_TOPDIR=googletest-release-1.8.0
DESTDIR=$HERE/../src/dep/

if [ -d $DESTDIR/gtest ]; then
    exit 0
fi

tarball=/tmp/${GTEST_FILENAME}
url=${GTEST_URL_BASE}/${GTEST_FILENAME}
if [ ! -e $tarball ]; then
    wget -q -O $tarball $url
fi

cd $DESTDIR
tar -xzf $tarball
rm -f gtest
ln -s $GTEST_TOPDIR gtest
