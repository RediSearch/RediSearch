#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# Full system upgrade — real-only (not a dep; `dnf install` doesn't need it, so
# list/dry-run stay clean). Runs on a real bootstrap.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then $MODE dnf update -y; fi

# Development Tools includes config-manager. Skip it once the core compiler
# trio is present so re-runs / dry-run don't keep re-listing the large group.
if ! rpm -q gcc gcc-c++ make >/dev/null 2>&1; then
    _sh "$MODE dnf groupinstall \"Development Tools\" -yqq"
fi

# powertools (Rocky/Alma) or codeready-builder (RHEL) is needed to install epel
dnf repolist --enabled 2>/dev/null | grep -qiE 'powertools|crb|codeready' || \
    _sh "$MODE dnf config-manager --set-enabled powertools 2>/dev/null || $MODE dnf config-manager --set-enabled \"codeready-builder-for-rhel-8-\$(uname -m)-rpms\" 2>/dev/null || true"

# get epel to install gcc13
rpm -q epel-release >/dev/null 2>&1 || _run dnf install epel-release -yqq

dnf_install gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ \
    gcc-toolset-13-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync \
    clang curl clang-devel lld gdb

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [ "$(uname -m)" = "aarch64" ]; then
    dnf_install python3.12-devel
fi

# Symlink the toolset compiler into /usr/local/bin — skip once gcc already points there.
if [ "$(readlink -f /usr/local/bin/gcc 2>/dev/null)" != /opt/rh/gcc-toolset-13/root/usr/bin/gcc ]; then
    _run cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
    _run ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/gcc  /usr/local/bin/gcc  || true
    _run ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/g++  /usr/local/bin/g++  || true
    _run ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/cc   /usr/local/bin/cc   || true
    _run ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/as   /usr/local/bin/as   || true
    _run ln -sf /opt/rh/gcc-toolset-13/root/usr/bin/make /usr/local/bin/make || true
fi
