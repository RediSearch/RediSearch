#!/usr/bin/env bash
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"

# Add a Launchpad PPA without add-apt-repository's `ppa:` shortcut.
#
# The `ppa:owner/name` shortcut resolves the PPA through the launchpad.net web
# frontend and imports its signing key from there. That host is intermittently
# unreachable from CI runners (TCP connections to it hang/time out), which makes
# the focal container build fail with "retrieving gpg key timed out" /
# "NO_PUBKEY" / "user or team does not exist" - all symptoms of the same failed
# launchpad.net call.
#
# Instead we avoid launchpad.net entirely and use only its reachable
# infrastructure: look up the signing-key fingerprint via the API host
# (api.launchpad.net), fetch the *public* signing key from the Ubuntu keyserver,
# then add the PPA as a plain `deb` line (no `ppa:` resolution). Packages still
# come from the reachable ppa.launchpad.net. The key is fetched before the source
# line is trusted, so a failed fetch never leaves a half-configured, unsigned
# source behind (which would otherwise poison retries).
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

    # `deb` line (not `ppa:`) => add-apt-repository just writes the source, with
    # no launchpad.net lookup. The key is already trusted via signed-by.
    $MODE add-apt-repository -y \
        "deb [signed-by=${keyring}] http://ppa.launchpad.net/${owner}/${name}/ubuntu ${codename} main"
}

apt_get_cmd "$MODE" update -qq
apt_get_cmd "$MODE" upgrade -yqq

# Provides add-apt-repository (software-properties-common) plus curl/gnupg used
# by add_launchpad_ppa below.
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
