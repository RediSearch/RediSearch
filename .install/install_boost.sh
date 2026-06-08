#!/usr/bin/env bash

set -eo pipefail
VERSION=1.88.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
else
    # Retry/time-out flags so a transient GitHub outage (e.g. a stalled release
    # download that just hangs on "Trying <ip>:443") fails fast and retries
    # instead of blocking the job until it times out.
    #   --tries / --waitretry / --retry-connrefused: retry transient failures
    #   --timeout: cap connect/read time so a stalled connection is abandoned
    wget --tries=5 --waitretry=10 --retry-connrefused --timeout=60 \
        https://github.com/boostorg/boost/releases/download/boost-${VERSION}/boost-${VERSION}-b2-nodocs.tar.gz -O ${BOOST_NAME}.tar.gz
    tar -xzf ${BOOST_NAME}.tar.gz
    mv boost-${VERSION} ${BOOST_DIR}
    rm ${BOOST_NAME}.tar.gz
fi
