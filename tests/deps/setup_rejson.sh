#!/bin/bash

# Set the default variables
ROOT ?= `pwd`
BINROOT ?= ${ROOT}/bin/linux-x64-release

JSON_BRANCH=${REJSON_BRANCH:-master}
JSON_REPO_URL="https://github.com/RedisJSON/RedisJSON.git"
TEST_DEPS_DIR="${ROOT}/tests/deps"
JSON_MODULE_DIR="${TEST_DEPS_DIR}/RedisJSON"
JSON_BIN_DIR="${BINROOT}/RedisJSON/${JSON_BRANCH}"
export JSON_BIN_PATH="${JSON_BIN_DIR}/rejson.so"

# Check if REJSON_PATH is set externally
if [ -n "$REJSON_PATH" ]; then
    JSON_BIN_PATH="$REJSON_PATH"
    echo "Using RedisJSON path given as REJSON_PATH: $REJSON_PATH"
    return 0
fi

# Clone the RedisJSON repository if it doesn't exist
if [ ! -d "${JSON_MODULE_DIR}" ]; then
    echo "Cloning RedisJSON repository from ${JSON_REPO_URL} to ${JSON_MODULE_DIR}..."
    git clone --quiet --recursive $JSON_REPO_URL $JSON_MODULE_DIR
    echo "Done"
else
    echo "RedisJSON already exists in ${JSON_MODULE_DIR}"
fi

# Navigate to the module directory and checkout the specified branch and its submodules
cd ${JSON_MODULE_DIR}
git checkout --quiet ${JSON_BRANCH}
git submodule update --quiet --init --recursive

# Build the RedisJSON module, use nightly toolchain if running sanitizer is needed.
if [[ -n $SAN ]]; then
  rustup component add rust-src --toolchain nightly
fi
echo "Building RedisJSON module for branch $JSON_BRANCH..."
BINROOT=${JSON_BIN_DIR} make SAN=$SAN > /dev/null  # print errors to console

echo "RedisJSON module built and is available at ${JSON_BIN_PATH}"
