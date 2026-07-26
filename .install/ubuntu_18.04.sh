#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

# debconf pre-seed — real-only (silent + non-mutating in list/dry; not a dep).
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then
    echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections 2>/dev/null || true
    echo 'tzdata tzdata/Areas select Etc' | debconf-set-selections 2>/dev/null || true
    echo 'tzdata tzdata/Zones/Etc select UTC' | debconf-set-selections 2>/dev/null || true
fi
apt_install gnupg wget software-properties-common unzip rsync gpg
# gcc-11 + a modern git come from the toolchain-r / git-core PPAs plus a system
# refresh. This raw prep is only needed until gcc-11 is installed, so gate it on
# gcc-11's absence — a re-run / dry-run on a provisioned host skips it entirely.
if ! dpkg-query -W -f='${Status}' gcc-11 2>/dev/null | grep -q 'ok installed'; then
    _sh "wget -qO- \"https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x1E9377A2BA9EF27F\" | $MODE gpg --batch --no-tty --yes --dearmor -o /etc/apt/trusted.gpg.d/ubuntu-toolchain-r.gpg || true"
    _sh "wget -qO- \"https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2C277A0A352154E5\" | $MODE gpg --batch --no-tty --yes --dearmor -o /etc/apt/trusted.gpg.d/ubuntu-toolchain-r-2.gpg || true"
    _sh "echo \"deb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu bionic main\" | $MODE tee /etc/apt/sources.list.d/ubuntu-toolchain-r-test.list"
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" update -qq
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" upgrade -yqq
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" dist-upgrade -yqq
    _sh "$MODE add-apt-repository ppa:git-core/ppa -y || true"
    _run apt-get -o DPkg::Lock::Timeout="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}" update
fi
apt_install build-essential git wget make gcc-11 g++-11 openssl libssl-dev curl libclang-dev clang gdb
# Only move the active compiler up, never down — another module's bootstrap
# may have already pinned something newer in this shared build container.
_gcc_cur=$(gcc -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
_gpp_cur=$(g++ -dumpversion 2>/dev/null | cut -d. -f1 || echo 0)
if [ "$_gcc_cur" -lt 11 ]; then
    # Register cc/gcc/g++ separately (no --slave grouping): if a debian-family
    # module ran earlier in this shared build container, its debian_default_install
    # already made g++ an independent master alternative, and --slave-grouping
    # it under gcc here would conflict with that ("g++ ... is a master alternative").
    _run update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-11 100
    _run update-alternatives --set     cc  /usr/bin/gcc-11
    _run update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
    _run update-alternatives --set     gcc /usr/bin/gcc-11
fi
if [ "$_gpp_cur" -lt 11 ]; then
    _run update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
    _run update-alternatives --set     g++ /usr/bin/g++-11
fi
