#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

# Add the PPA manually instead of via the `ppa:` shortcut, which relies on
# launchpad.net - intermittently unreachable from CI runners. See MOD-XXXXX.
add_launchpad_ppa() {
    local owner="$1" name="$2"
    local codename keyring fp
    codename="$(. /etc/os-release && echo "$VERSION_CODENAME")"
    keyring="/etc/apt/keyrings/${owner}-${name}.gpg"

    fp="$(curl -fsSL --retry 5 --retry-delay 5 --retry-connrefused \
        "https://api.launchpad.net/devel/~${owner}/+archive/ubuntu/${name}" \
        | grep -o '"signing_key_fingerprint":[[:space:]]*"[^"]*"' | cut -d'"' -f4)"
    if [ -z "$fp" ]; then
        echo "ERROR: could not resolve signing key fingerprint for ppa:${owner}/${name}" >&2
        return 1
    fi

    $MODE install -d -m 0755 /etc/apt/keyrings
    curl -fsSL --retry 5 --retry-delay 5 --retry-connrefused \
        "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x${fp}" \
        | $MODE gpg --dearmor --yes -o "$keyring"

    $MODE add-apt-repository -y \
        "deb [signed-by=${keyring}] http://ppa.launchpad.net/${owner}/${name}/ubuntu ${codename} main"
}

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" upgrade -yqq

# software-properties-common provides add-apt-repository; curl/gnupg used by add_launchpad_ppa
apt_get_cmd "$MODE" install -yqq software-properties-common curl gnupg

add_launchpad_ppa ubuntu-toolchain-r test
add_launchpad_ppa deadsnakes ppa

apt_get_cmd "$MODE" install -yqq wget make clang-format gcc lcov git openssl libssl-dev \
    unzip rsync build-essential gcc-11 g++-11 curl libclang-dev gdb

$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 --slave /usr/bin/g++ g++ /usr/bin/g++-11
# Align gcov version with gcc version
$MODE update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-11 60

# Need clang for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
