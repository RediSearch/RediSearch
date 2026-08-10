#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

apt_install gcc-12 g++-12 git wget build-essential lcov openssl libssl-dev \
    unzip rsync curl gdb
# Only move the active compiler up, never down — another module's bootstrap
# may have already pinned something newer in this shared build container.
_gcc_cur=$(gcc -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
_gpp_cur=$(g++ -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
if (( _gcc_cur < 12 )); then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING gcc-active:12"
    else
        _run update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-12 60
        _run update-alternatives --set     cc  /usr/bin/gcc-12
        _run update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 60
        _run update-alternatives --set     gcc /usr/bin/gcc-12
        # Align gcov version with gcc version
        _run update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-12 60
        _run update-alternatives --set     gcov /usr/bin/gcov-12
    fi
fi
if (( _gpp_cur < 12 )); then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING g++-active:12"
    else
        _run update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 60
        _run update-alternatives --set     g++ /usr/bin/g++-12
    fi
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
