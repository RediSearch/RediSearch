#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" install -yqq git wget build-essential lcov openssl libssl-dev \
        rsync unzip curl gdb

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# Debian 11 ships GCC 10 which cmake picks by default. GCC 10 < 11 disables SVS
# (C++20 requirement), causing build failures. Register the just-installed clang
# with higher priority so cmake auto-detects it instead of GCC 10.
# Also, GCC 10's libstdc++ lacks std::bit_cast (added in GCC 11); install libc++
# from the LLVM repo and configure clang++ to use it via a driver config file.
source "$(dirname "${BASH_SOURCE[0]}")/LLVM_VERSION.sh"
${MODE} update-alternatives --install /usr/bin/cc  cc  "/usr/bin/clang-${LLVM_VERSION}"   100
${MODE} update-alternatives --install /usr/bin/c++ c++ "/usr/bin/clang++-${LLVM_VERSION}" 100

apt_get_cmd "$MODE" install -yqq "libc++-${LLVM_VERSION}-dev" "libc++abi-${LLVM_VERSION}-dev"

# clang auto-loads <driver-dir>/<driver-stem>.cfg; write it next to the real binary.
CLANG_BIN_DIR=$(dirname "$(readlink -f "/usr/bin/clang++-${LLVM_VERSION}")")
echo "-stdlib=libc++" | ${MODE} tee "${CLANG_BIN_DIR}/clang++.cfg" > /dev/null

# Register clang as gcc/g++ too so jemalloc (which hardcodes CC=gcc in its configure)
# also compiles with LLVM, keeping LTO IR consistent with the rest of the build.
${MODE} update-alternatives --install /usr/bin/gcc gcc "/usr/bin/clang-${LLVM_VERSION}"   100
${MODE} update-alternatives --install /usr/bin/g++ g++ "/usr/bin/clang++-${LLVM_VERSION}" 100
