#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail
# Full system upgrade — real-only (not a dep; `dnf install` doesn't need it, so
# list/dry-run stay clean). Runs on a real bootstrap.
if [[ "${CHECK_DEPS:-0}" != 1 && "${DRY_RUN:-0}" != 1 ]]; then $MODE dnf update -y; fi

dnf_install gcc-toolset-14-gcc gcc-toolset-14-gcc-c++ make wget git

# Add to profile for _future_ shells — skip once the snippet is already there.
if [[ ! -f /etc/profile.d/gcc-toolset-14.sh ]]; then
    _run cp /opt/rh/gcc-toolset-14/enable /etc/profile.d/gcc-toolset-14.sh
fi
# Source for _this_ shell once available; dry-run prints the same pasted step.
_env '[[ -f /opt/rh/gcc-toolset-14/enable ]] && source /opt/rh/gcc-toolset-14/enable || true'

# install other stuff after installing gcc-toolset-14 to avoid dependencies conflicts
dnf_install openssl openssl-devel which rsync unzip curl gdb xz

# Native EL9 clang/lld packages carry the matching runtime assumptions. Avoid
# pulling Fedora libstdc++ into the system package set; it conflicts with EL9
# gcc-c++/annobin and can leave dnf skipping the required upgrade.

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
