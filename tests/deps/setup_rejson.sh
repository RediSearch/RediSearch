#!/usr/bin/env bash
set -e

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

JSON_MODULE_DIR="${ROOT}/deps/RedisJSON"
JSON_BIN_DIR="${BINROOT}/RedisJSON"
export JSON_BIN_PATH="${JSON_BIN_DIR}/rejson.so"
# Instruct RedisJSON to use the same pinned nightly version as RediSearch
export RUST_GOOD_NIGHTLY=$(cat ${ROOT}/.rust-nightly)

# Check if REJSON_PATH is set externally
if [ -n "$REJSON_PATH" ]; then
    JSON_BIN_PATH="$REJSON_PATH"
    echo "Using RedisJSON path given as REJSON_PATH: $REJSON_PATH"
    return 0
fi

# Navigate to the module directory and checkout the specified branch and its submodules
cd ${JSON_MODULE_DIR}

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

echo "Building RedisJSON module..."
run_command make SAN=$SAN BINROOT=${JSON_BIN_DIR}
echo "RedisJSON module built and is available at ${JSON_BIN_PATH}"
cd $CURR_DIR
