#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
# Full system upgrade — real-only (not a dep; `dnf install` doesn't need it, so
# list/dry-run stay clean). Runs on a real bootstrap.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then $MODE dnf update -y; fi

dnf_install gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git

# Add to profile for _future_ shells — skip once the snippet is already there.
if [ ! -f /etc/profile.d/gcc-toolset-14.sh ]; then
    _run cp /opt/rh/gcc-toolset-14/enable /etc/profile.d/gcc-toolset-14.sh
fi
# Source for _this_ shell (only once the toolset is actually installed).
[ -f /opt/rh/gcc-toolset-14/enable ] && source /opt/rh/gcc-toolset-14/enable

# install other stuff after installing gcc-toolset-14 to avoid dependencies conflicts
dnf_install openssl openssl-devel which rsync unzip curl gdb xz

# The LLVM tarball binaries need GLIBCXX_3.4.30+ but Rocky/RHEL 9's system
# libstdc++ (GCC 11) only provides up to GLIBCXX_3.4.29. Install a newer
# libstdc++ runtime from Fedora 43. --allowerasing lets dnf remove conflicting
# packages (e.g. annobin, which requires gcc < 12) to satisfy the upgrade.
_sh "$MODE dnf install -y --repofrompath=fedora,'https://dl.fedoraproject.org/pub/fedora/linux/releases/43/Everything/\$basearch/os/' --setopt=fedora.gpgcheck=0 --disablerepo='*' --enablerepo=fedora --skip-broken libstdc++ || true"

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
