#!/usr/bin/env bash

# Function to run a command, and only if it fails, print stdout and stderr and then exit
run_command() {
  output=$(eval "$@" 2>&1)
  status=$?
  if [ $status -ne 0 ]; then
    echo "$output"
    exit $status
  fi
}

# Set the default variables
CURR_DIR=`pwd`
ROOT=${ROOT:=$CURR_DIR}  # unless ROOT is set, assume it is the current directory
BINROOT=${BINROOT:=${ROOT}/bin/linux-x64-release}

JSON_BRANCH=${REJSON_BRANCH:-master}
JSON_REPO_URL="https://github.com/RedisJSON/RedisJSON.git"
TEST_DEPS_DIR="${ROOT}/tests/deps"
JSON_MODULE_DIR="${TEST_DEPS_DIR}/RedisJSON"
JSON_BIN_DIR="${BINROOT}/RedisJSON/${JSON_BRANCH}"
export JSON_BIN_PATH="${JSON_BIN_DIR}/rejson.so"
# Instruct RedisJSON to use the same pinned nightly version as RediSearch
export RUST_GOOD_NIGHTLY=$(grep "NIGHTLY_VERSION=" ${ROOT}/build.sh | cut -d'=' -f2 | tr -d '"')

# Check if REJSON_PATH is set externally
if [ -n "$REJSON_PATH" ]; then
    JSON_BIN_PATH="$REJSON_PATH"
    echo "Using RedisJSON path given as REJSON_PATH: $REJSON_PATH"
    return 0
fi

# Clone the RedisJSON repository if it doesn't exist
if [ ! -d "${JSON_MODULE_DIR}" ]; then
    echo "Cloning RedisJSON repository from ${JSON_REPO_URL} to ${JSON_MODULE_DIR}..."
    run_command git clone --quiet --recursive $JSON_REPO_URL $JSON_MODULE_DIR
    echo "Done"
else
    echo "RedisJSON already exists in ${JSON_MODULE_DIR}"
fi

# Navigate to the module directory and checkout the specified branch and its submodules
cd ${JSON_MODULE_DIR}
run_command git checkout --quiet ${JSON_BRANCH}
run_command git submodule update --quiet --init --recursive

# Patch RedisJSON to build in Alpine - disable static linking
# This is to fix RedisJSON build in Alpine, which is used only for testing
# See https://github.com/rust-lang/rust/pull/58575#issuecomment-496026747
if [[ -f /etc/os-release ]]; then
	OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
	OS_NAME=${OS_NAME#"NAME="}
	if [[ $OS_NAME == "Alpine Linux" ]]; then
	  run_command "sed -i 's/^RUST_FLAGS=$/RUST_FLAGS=-C target-feature=-crt-static/g' Makefile"
	fi
fi

echo "Building RedisJSON module for branch $JSON_BRANCH..."
run_command make SAN=$SAN BINROOT=${JSON_BIN_DIR}
echo "RedisJSON module built and is available at ${JSON_BIN_PATH}"
cd $CURR_DIR
