#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -eo pipefail

# Full system upgrade — real-only (not a dep; `dnf install` doesn't need it, so
# list/dry-run stay clean). Runs on a real bootstrap.
if [[ "${CHECK_DEPS:-0}" != 1 && "${DRY_RUN:-0}" != 1 ]]; then $MODE dnf update -y; fi

# `dnf config-manager` is needed before enabling powertools/codeready.
dnf_install dnf-plugins-core

# Keep the large group out of list mode; package checks below cover build deps.
if [[ "${CHECK_DEPS:-0}" != 1 ]] && ! rpm -q gcc gcc-c++ make >/dev/null 2>&1; then
    _sh "$MODE dnf groupinstall \"Development Tools\" -yqq < /dev/null"
fi

# powertools (Rocky/Alma) or codeready-builder (RHEL) is needed to install epel
dnf repolist --enabled 2>/dev/null | grep -qiE 'powertools|crb|codeready' || \
    _sh "$MODE dnf config-manager --set-enabled powertools 2>/dev/null || $MODE dnf config-manager --set-enabled \"codeready-builder-for-rhel-8-\$(uname -m)-rpms\" 2>/dev/null || true"

# get epel to install gcc13 (dnf_install records it in list + is idempotent in real)
dnf_install epel-release

dnf_install gcc-toolset-13-gcc gcc-toolset-13-gcc-c++ \
    gcc-toolset-13-libatomic-devel make wget git openssl openssl-devel \
    bzip2-devel libffi-devel zlib-devel tar xz which rsync \
    clang curl clang-devel lld gdb

# We need Python headers to build psutil@5.x.y from
# source, since it only started providing wheels for aarch64
# in version 6.w.z.
if [[ "$(uname -m)" == "aarch64" ]]; then
    dnf_install python3.12-devel
fi

# Symlink the toolset compiler into /usr/local/bin.
_toolset13_bindir=/opt/rh/gcc-toolset-13/root/usr/bin
_toolset13_shim_ok() {
    [[ "$(readlink -f "/usr/local/bin/$1" 2>/dev/null)" == "$_toolset13_bindir/$2" ]]
}
_toolset13_needs_shims=0
[[ -f /etc/profile.d/gcc-toolset-13.sh ]] || _toolset13_needs_shims=1
_toolset13_shim_ok gcc gcc || _toolset13_needs_shims=1
_toolset13_shim_ok g++ g++ || _toolset13_needs_shims=1
_toolset13_shim_ok cc cc || _toolset13_needs_shims=1
_toolset13_shim_ok as as || _toolset13_needs_shims=1
_toolset13_shim_ok make make || _toolset13_needs_shims=1

if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
    if [[ "$_toolset13_needs_shims" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING gcc-toolset-13-shims"
    else
        DEPS_OK="$DEPS_OK gcc-toolset-13-shims"
    fi
else
    [[ -f /etc/profile.d/gcc-toolset-13.sh ]] || _run cp /opt/rh/gcc-toolset-13/enable /etc/profile.d/gcc-toolset-13.sh
    _toolset13_shim_ok gcc gcc || _run ln -sf "$_toolset13_bindir/gcc" /usr/local/bin/gcc
    _toolset13_shim_ok g++ g++ || _run ln -sf "$_toolset13_bindir/g++" /usr/local/bin/g++
    _toolset13_shim_ok cc cc || _run ln -sf "$_toolset13_bindir/cc" /usr/local/bin/cc
    _toolset13_shim_ok as as || _run ln -sf "$_toolset13_bindir/as" /usr/local/bin/as
    _toolset13_shim_ok make make || _run ln -sf "$_toolset13_bindir/make" /usr/local/bin/make
fi
