#!/usr/bin/env bash

set -e
VERSION=1.88.0
BOOST_NAME="boost_${VERSION//./_}"
BOOST_DIR="boost" # here we search for the boost cached installation if exists. Do not change this value

if [[ -d ${BOOST_DIR} ]]; then
    echo "Boost cache directory present, skipping installation"
    exit 0
fi

BOOST_URL="https://github.com/boostorg/boost/releases/download/boost-${VERSION}/boost-${VERSION}-b2-nodocs.tar.gz"
# Retry the download so a transient GitHub outage (e.g. a stalled release
# download that just hangs on "Trying <ip>:443") is retried instead of
# failing the job on the first blip. The retry loop is in shell rather than
# via wget flags because this script also runs on Alpine, whose BusyBox
# wget rejects GNU-only options like --tries/--waitretry/--retry-connrefused.
# `-T 60` (network timeout) is supported by both GNU and BusyBox wget, so a
# stalled connection is abandoned and retried rather than hanging.
for attempt in 1 2 3 4 5; do
    wget -T 60 "${BOOST_URL}" -O ${BOOST_NAME}.tar.gz && break
    if [ "$attempt" -eq 5 ]; then
        echo "Boost download failed after $attempt attempts" >&2
        exit 1
    fi
    echo "Boost download failed (attempt $attempt), retrying in 10s..." >&2
    sleep 10
done

tar -xzf ${BOOST_NAME}.tar.gz
mv boost-${VERSION} ${BOOST_DIR}
rm ${BOOST_NAME}.tar.gz
