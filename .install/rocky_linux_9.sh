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

# The LLVM tarball binaries need GLIBCXX_3.4.30+ but Rocky/RHEL 9's system
# libstdc++ (GCC 11) only provides up to GLIBCXX_3.4.29. Install a newer
# libstdc++ runtime from Fedora 43. --allowerasing lets dnf remove conflicting
# packages (e.g. annobin, which requires gcc < 12) to satisfy the upgrade.
_libstdcxx_has_glibcxx_3_4_30() {
    local lib
    for lib in /usr/lib64/libstdc++.so.6 /usr/lib/libstdc++.so.6; do
        [[ -e "$lib" ]] || continue
        grep -aq 'GLIBCXX_3\.4\.30' "$lib" && return 0
    done
    return 1
}

if ! _libstdcxx_has_glibcxx_3_4_30; then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING libstdc++:GLIBCXX_3.4.30"
    else
        _sh "$MODE dnf install -y --repofrompath=fedora,'https://dl.fedoraproject.org/pub/fedora/linux/releases/43/Everything/\$basearch/os/' --setopt=fedora.gpgcheck=0 --disablerepo='*' --enablerepo=fedora --skip-broken libstdc++ < /dev/null"
        if [[ "${DRY_RUN:-0}" != 1 ]] && ! _libstdcxx_has_glibcxx_3_4_30; then
            echo "ERROR: libstdc++ still lacks GLIBCXX_3.4.30 after runtime upgrade" >&2
            return 1 2>/dev/null || exit 1
        fi
    fi
else
    [[ "${CHECK_DEPS:-0}" == 1 ]] && DEPS_OK="$DEPS_OK libstdc++"
fi

# Install LLVM for LTO
source "$(dirname "${BASH_SOURCE[0]}")/install_llvm.sh" $MODE
