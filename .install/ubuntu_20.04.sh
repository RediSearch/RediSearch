#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" upgrade -yqq

# Provides the add-apt-repository command
# Ensure dbus system socket exists before apt installs that trigger dbus-user-session post-install
$MODE mkdir -p /var/run/dbus
$MODE dbus-daemon --system --fork 2>/dev/null || true

apt_get_cmd "$MODE" install -yqq software-properties-common

echo "deb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu focal main" | $MODE tee /etc/apt/sources.list.d/ubuntu-toolchain-r-test.list
$MODE apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 1E9377A2BA9EF27F
echo "deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu focal main" | $MODE tee /etc/apt/sources.list.d/deadsnakes.list
$MODE apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F23C5A6CF475977595C89F51BA6932366A755776

apt_get_cmd "$MODE" update -qq

apt_get_cmd "$MODE" install -yqq wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev gdb

# Only move the active compiler up, never down — another module's bootstrap
# may have already pinned something newer in this shared build container.
_cur=$(gcc -dumpversion | cut -d. -f1)
if [ "$_cur" -lt 11 ]; then
    $MODE update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-11 100
    $MODE update-alternatives --set     cc  /usr/bin/gcc-11
    $MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
    $MODE update-alternatives --set     gcc /usr/bin/gcc-11
    $MODE update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
    $MODE update-alternatives --set     g++ /usr/bin/g++-11
    # Align gcov version with gcc version
    $MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 100
    $MODE update-alternatives --set     gcov /usr/bin/gcov-11
fi

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
