#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# tdnf intermittently fails on Azure Linux mirrors with
# "TDNFVerifySignature 2004" / "Failed to synchronize cache". Retry, and
# within each attempt fall back to skipping the repo-metadata GPG plugin
# (per-package GPG checks still apply).
tdnf_install() {
    local i
    for i in 1 2 3; do
        $MODE tdnf install -y "$@" && return 0
        $MODE tdnf --disableplugin=tdnfrepogpgcheck install -y "$@" && return 0
        [ "$i" -lt 3 ] && sleep 10
    done
    return 1
}

tdnf_install -q build-essential ca-certificates gdb git libxcrypt-devel openssl-devel rsync tar unzip wget which xz

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    tdnf_install python3-devel
fi
