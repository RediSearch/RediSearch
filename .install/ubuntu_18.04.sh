#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

# apt_get_cmd update runs before installing gnupg/wget so this works on a
# fresh base image with no apt index yet.
export DEBIAN_FRONTEND=noninteractive
echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections 2>/dev/null || true
echo 'tzdata tzdata/Areas select Etc' | debconf-set-selections 2>/dev/null || true
echo 'tzdata tzdata/Zones/Etc select UTC' | debconf-set-selections 2>/dev/null || true
apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" install -yqq --no-install-recommends gnupg wget
wget -qO- "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x1E9377A2BA9EF27F" | $MODE gpg --batch --no-tty --yes --dearmor -o /etc/apt/trusted.gpg.d/ubuntu-toolchain-r.gpg || true
wget -qO- "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2C277A0A352154E5" | $MODE gpg --batch --no-tty --yes --dearmor -o /etc/apt/trusted.gpg.d/ubuntu-toolchain-r-2.gpg || true
echo "deb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu bionic main" | $MODE tee /etc/apt/sources.list.d/ubuntu-toolchain-r-test.list

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" upgrade -yqq
apt_get_cmd "$MODE" dist-upgrade -yqq
apt_get_cmd "$MODE" install -yqq software-properties-common unzip rsync wget gpg
$MODE add-apt-repository ppa:git-core/ppa -y || true
apt_get_cmd "$MODE" update
apt_get_cmd "$MODE" install -yqq build-essential git wget make gcc-11 g++-11 openssl libssl-dev curl libclang-dev clang gdb
# Only move the active compiler up, never down — another module's bootstrap
# may have already pinned something newer in this shared build container.
_gcc_cur=$(gcc -dumpversion | cut -d. -f1)
_gpp_cur=$(g++ -dumpversion | cut -d. -f1)
if [ "$_gcc_cur" -lt 11 ]; then
    # Register cc/gcc/g++ separately (no --slave grouping): if a debian-family
    # module ran earlier in this shared build container, its debian_default_install
    # already made g++ an independent master alternative, and --slave-grouping
    # it under gcc here would conflict with that ("g++ ... is a master alternative").
    $MODE update-alternatives --install /usr/bin/cc  cc  /usr/bin/gcc-11 100
    $MODE update-alternatives --set     cc  /usr/bin/gcc-11
    $MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
    $MODE update-alternatives --set     gcc /usr/bin/gcc-11
fi
if [ "$_gpp_cur" -lt 11 ]; then
    $MODE update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
    $MODE update-alternatives --set     g++ /usr/bin/g++-11
fi
