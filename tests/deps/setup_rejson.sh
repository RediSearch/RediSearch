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

# RedisJSON's readies-driven build below requires a pip-capable python3,
# which system interpreters no longer guarantee (PEP 668). The repo venv
# provisioned by .install/test_deps/install_python_deps.sh has pip seeded;
# activate it when the calling shell has not done so already.
if [[ -z "$VIRTUAL_ENV" && -f "${ROOT}/.venv/bin/activate" ]]; then
    source "${ROOT}/.venv/bin/activate"
fi

JSON_BRANCH=${REJSON_BRANCH:-master}
JSON_REPO_URL="https://github.com/RedisJSON/RedisJSON.git"
TEST_DEPS_DIR="${ROOT}/tests/deps"
JSON_MODULE_DIR="${TEST_DEPS_DIR}/RedisJSON"
JSON_BIN_DIR="${BINROOT}/RedisJSON/${JSON_BRANCH}"
export JSON_BIN_PATH="${JSON_BIN_DIR}/rejson.so"
# Instruct RedisJSON to use the same pinned nightly version as RediSearch
export RUST_GOOD_NIGHTLY=$(cat ${ROOT}/.rust-nightly)

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
    cd ${JSON_MODULE_DIR}
    run_command git pull --quiet
    cd -
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

# [DEBUG MOD-16514 — temporary] Capture the python3/pip state readies getpy3
# will see for this RedisJSON build (the actual failure point). Revert before merge.
if [[ -f /etc/alpine-release ]]; then
	echo "===== PIP DIAG (before RedisJSON make) ====="
	echo "PATH=$PATH"
	echo "type -a python3:"; type -a python3 2>&1 || true
	echo "ls -la .venv/bin/python*:"; ls -la "${GITHUB_WORKSPACE:-$ROOT}/.venv/bin/"python* 2>&1 || true
	p3="$(command -v python3)"; echo "command -v python3: $p3 | readlink -f: $(readlink -f "$p3" 2>&1)"
	python3 - <<'PYEOF' 2>&1 || true
import sys, os, importlib.util, traceback
print("version:", sys.version.replace(chr(10), " "))
print("executable:", sys.executable)
print("prefix:", sys.prefix, "| base_prefix:", sys.base_prefix)
print("VIRTUAL_ENV:", os.environ.get("VIRTUAL_ENV"), "| LD_LIBRARY_PATH:", os.environ.get("LD_LIBRARY_PATH"))
print("sys.path:", sys.path)
print("find_spec(pip):", importlib.util.find_spec("pip"))
try:
    import pip; print("import pip OK:", pip.__file__)
except Exception:
    print("import pip FAILED:"); traceback.print_exc()
PYEOF
	echo "-- python3 -m pip --version:"; python3 -m pip --version 2>&1 || true
	echo "===== END PIP DIAG ====="
fi

echo "Building RedisJSON module for branch $JSON_BRANCH..."
# RedisJSON's Makefile expects cargo to write to `$(BINDIR)/target/release/`
# (no target-triple subdirectory). But build.sh exports
# `CARGO_BUILD_TARGET=<host triple>` when SAN=address (so sanitizer rustflags
# don't reach build scripts), which makes cargo write to
# `$(BINDIR)/target/<triple>/release/` instead and the Makefile's subsequent
# `cp` fails with "No such file or directory". Scrub CARGO_BUILD_TARGET for
# this invocation; RedisJSON manages its own toolchain/target.
run_command env -u CARGO_BUILD_TARGET make SAN=$SAN BINROOT=${JSON_BIN_DIR}
echo "RedisJSON module built and is available at ${JSON_BIN_PATH}"
cd $CURR_DIR
