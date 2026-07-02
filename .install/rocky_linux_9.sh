#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git --nobest --skip-broken --allowerasing

# Source for _this_ shell only, so the rest of the bootstrap (e.g. boost)
# builds with it. Not copied to /etc/profile.d: that would silently change
# the default compiler for every other checkout on this host. build.sh
# enables the toolset itself when CC is unset.
source /opt/rh/gcc-toolset-14/enable

# install other stuff after installing gcc-toolset-14 to avoid dependencies conflicts
$MODE dnf install -y openssl openssl-devel which rsync unzip curl gdb xz --nobest --skip-broken --allowerasing

# The LLVM tarball binaries need GLIBCXX_3.4.30+ but Rocky/RHEL 9's system
# libstdc++ (GCC 11) only provides up to GLIBCXX_3.4.29. Install a newer
# libstdc++ runtime from Fedora 43. --allowerasing lets dnf remove conflicting
# packages (e.g. annobin, which requires gcc < 12) to satisfy the upgrade.
$MODE dnf install -y \
    --repofrompath=fedora,'https://dl.fedoraproject.org/pub/fedora/linux/releases/43/Everything/$basearch/os/' \
    --setopt=fedora.gpgcheck=0 --disablerepo='*' --enablerepo=fedora \
    --skip-broken libstdc++ || true

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
