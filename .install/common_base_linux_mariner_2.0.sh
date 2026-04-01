#!/usr/bin/env bash
# Common Base Linux Mariner is the actual full name of the platform.
# Don't attempt to change the name of this file.
MODE=$1 # whether to install using sudo or not
set -eo pipefail
export DEBIAN_FRONTEND=noninteractive
$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip xz rsync \
                         openssl-devel openssl which gzip gdb curl binutils perl

# The LLVM tarball binaries need GLIBCXX_3.4.30+ but Mariner 2's system
# libstdc++ (GCC 11) only provides up to GLIBCXX_3.4.29. Install a newer
# libstdc++ runtime (.so) from Fedora 36 which provides up to GLIBCXX_3.4.30.
# Fedora 36 _is_ EOL, but was chosen because its version of glibc (2.35)
# matches that of Mariner 2.
ARCH=$(uname -m)
FEDORA_LIBSTDCXX_URL="https://archives.fedoraproject.org/pub/archive/fedora/linux/releases/36/Everything/${ARCH}/os/Packages/l/libstdc++-12.0.1-0.16.fc36.${ARCH}.rpm"
curl -fSL -o /tmp/libstdcpp.rpm "$FEDORA_LIBSTDCXX_URL"
$MODE rpm -Uvh --force --nodeps /tmp/libstdcpp.rpm
rm -f /tmp/libstdcpp.rpm

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
