#!/bin/sh
# Run this from the toplevel directory of the source code tree
GTEST_URL_BASE=https://s3-eu-central-1.amazonaws.com/redislabs-dev-public-deps
GTEST_URL_BASE=https://github.com/google/googletest/archive/
GTEST_FILENAME=release-1.8.0.zip
GTEST_TOPDIR=googletest-release-1.8.0
DESTDIR=src/dep/

if [ -d $DESTDIR/gtest ]; then
    exit 0
fi

curdir=$PWD
zipball=/tmp/${GTEST_FILENAME}
url=${GTEST_URL_BASE}/${GTEST_FILENAME}
if [ ! -e $zipball ]; then
    wget -O $zipball $url
fi

unzip -d $DESTDIR/ $zipball
rm $DESTDIR/gtest
cd $DESTDIR
ln -s $GTEST_TOPDIR gtest
