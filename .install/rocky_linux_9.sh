#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git --nobest --skip-broken --allowerasing

# Add to profile for _future_ shells
cp /opt/rh/gcc-toolset-14/enable /etc/profile.d/gcc-toolset-14.sh
# Source for _this_ shell
source /opt/rh/gcc-toolset-14/enable

# install other stuff after installing gcc-toolset-14 to avoid dependencies conflicts
$MODE dnf install -y openssl openssl-devel which rsync unzip curl gdb xz perl --nobest --skip-broken --allowerasing

# The LLVM tarball binaries need GLIBCXX_3.4.30+ but Rocky 9's system
# libstdc++ (GCC 11) only provides up to GLIBCXX_3.4.29, and gcc-toolset-14
# doesn't ship its own runtime libstdc++. Install a newer libstdc++ runtime
# (.so) from Fedora 43 which provides up to GLIBCXX_3.4.34.
# If this download starts to fail, it's probably because Fedora 43 has become EOL,
# which changes the URL.
$MODE dnf install -y --repofrompath=fedora,'https://dl.fedoraproject.org/pub/fedora/linux/releases/43/Everything/$basearch/os/' \
    --setopt=fedora.gpgcheck=0 --disablerepo='*' --enablerepo=fedora \
    libstdc++

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
