#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# gcc-11/g++-11 (toolchain-r PPA) + a newer python (deadsnakes PPA) come from
# added repos plus a system refresh + dbus prep. Gate on both compiler packages
# so partially provisioned hosts still get the repo setup needed for the missing
# half.
_need_gcc11_repo=0
if ! dpkg-query -W -f='${Status}' gcc-11 2>/dev/null | grep -q 'ok installed' ||
        ! dpkg-query -W -f='${Status}' g++-11 2>/dev/null | grep -q 'ok installed'; then
    _need_gcc11_repo=1
fi
if [[ "$_need_gcc11_repo" == 1 ]]; then
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" update -qq
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" upgrade -yqq
fi

# Ensure dbus system socket exists before apt installs that trigger dbus-user-session post-install
if [[ "${CHECK_DEPS:-0}" != 1 && ! -S /var/run/dbus/system_bus_socket ]]; then
    _run mkdir -p /var/run/dbus
    _sh "$MODE dbus-daemon --system --fork 2>/dev/null || true"
fi

apt_install software-properties-common

if [[ "$_need_gcc11_repo" == 1 ]]; then
    _sh "echo \"deb https://ppa.launchpadcontent.net/ubuntu-toolchain-r/test/ubuntu focal main\" | $MODE tee /etc/apt/sources.list.d/ubuntu-toolchain-r-test.list"
    # Import the ubuntu-toolchain-r/test PPA signing key.
    _run apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 1E9377A2BA9EF27F
    _sh "echo \"deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu focal main\" | $MODE tee /etc/apt/sources.list.d/deadsnakes.list"
    # Import the deadsnakes PPA signing key.
    _run apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F23C5A6CF475977595C89F51BA6932366A755776
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" update -qq
fi

apt_install wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev gdb

# Only move the active compiler up, never down — another module's bootstrap
# may have already pinned something newer in this shared build container.
_gcc_cur=$(gcc -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
_gpp_cur=$(g++ -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
if (( _gcc_cur < 11 )); then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING gcc-active:11"
    else
        _run update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-11 100
        _run update-alternatives --set     cc  /usr/bin/gcc-11
        _run update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
        _run update-alternatives --set     gcc /usr/bin/gcc-11
        # Align gcov version with gcc version
        _run update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 100
        _run update-alternatives --set     gcov /usr/bin/gcov-11
    fi
fi
if (( _gpp_cur < 11 )); then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING g++-active:11"
    else
        _run update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
        _run update-alternatives --set     g++ /usr/bin/g++-11
    fi
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
