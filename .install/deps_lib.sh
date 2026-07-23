#!/usr/bin/env bash
# Dependency-check / dry-run machinery + package-manager install primitives
# for RediSearch's bootstrap. Modeled on redistimeseries' lib/pm.sh, adapted
# to this module's framework (the sudo prefix is $MODE, set by install_script.sh
# from $1; scripts are `source`d, so early-exits use `return 0 2>/dev/null`).
#
# Sourced by install_script.sh (before the OS script) and defensively by the
# scripts install_script.sh runs as subprocesses (install_llvm.sh on macOS,
# test_deps/install_python_deps.sh). It self-guards against re-sourcing so the
# accumulated DEPS_* records are never reset.
#
# Exports:
#   PM    -> brew | apt | dnf | tdnf | yum | apk
#   SUDO  -> "$MODE" ("sudo" or "") on Linux, "" on macOS (brew refuses sudo)
#
# Primitives (filter-to-missing in dry-run, record in list, install otherwise):
#   apt_install / dnf_install / yum_install / tdnf_install / apk_install / brew_install

# Re-source guard: sourcing again must NOT reset DEPS_OK/DEPS_MISSING (that
# would drop records already accumulated by earlier-sourced scripts).
if [ -n "${_DEPS_LIB_SOURCED:-}" ]; then
    return 0 2>/dev/null || true
fi
_DEPS_LIB_SOURCED=1

# ----------------------------------------------------------------------------
# Package-manager + sudo-prefix detection
# ----------------------------------------------------------------------------
if [ "$(uname -s)" = "Darwin" ]; then
    PM="brew"; SUDO=""
elif command -v apt-get >/dev/null 2>&1; then PM="apt";  SUDO="${MODE:-}"
elif command -v dnf     >/dev/null 2>&1; then PM="dnf";  SUDO="${MODE:-}"
elif command -v tdnf    >/dev/null 2>&1; then PM="tdnf"; SUDO="${MODE:-}"
elif command -v yum     >/dev/null 2>&1; then PM="yum";  SUDO="${MODE:-}"
elif command -v apk     >/dev/null 2>&1; then PM="apk";  SUDO="${MODE:-}"
else
    echo "deps_lib.sh: no supported package manager (apt/dnf/tdnf/yum/apk/brew)" >&2
    return 1 2>/dev/null || exit 1
fi

CHECK_DEPS="${CHECK_DEPS:-0}"
DRY_RUN="${DRY_RUN:-0}"
DEPS_OK="${DEPS_OK:-}"
DEPS_MISSING="${DEPS_MISSING:-}"
DEPS_OPT_OK="${DEPS_OPT_OK:-}"
DEPS_OPT_MISSING="${DEPS_OPT_MISSING:-}"

# Optional deps: still installed by a real bootstrap; this only splits them
# into a separate "optional" list bucket and stops a missing one from failing
# `make bootstrap list`. Optional = tests/coverage/debug/feature extras that
# aren't build-critical (build-critical: gcc, g++, clang, cmake, make,
# openssl-dev, ...).
OPTIONAL_PKGS="${OPTIONAL_PKGS:-valgrind gdb lcov jq clang-format rsync unzip py3-numpy python3-numpy py3-psutil python3-psutil py3-cryptography python3-cryptography openssh xsimd openblas-dev curl tar uv awscli}"
_is_optional() { case " $OPTIONAL_PKGS " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# MIN_VERSIONS: sparse "pkg:minversion" list — only deps with a real floor a
# distro package can satisfy. cmake's >= 3.25 is provisioned by install_cmake.sh
# (recorded there), so it's intentionally NOT here.
MIN_VERSIONS="${MIN_VERSIONS:-gcc:10 g++:10}"
_min_for() { for _e in $MIN_VERSIONS; do case "$_e" in "$1:"*) echo "${_e#*:}"; return ;; esac; done; }

_get_installed_version() {
    case "$1" in
        gcc|g++) "$1" -dumpversion 2>/dev/null ;;
        *)       "$1" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 || true ;;
    esac
}

# version_ge HAVE WANT -> 0 if HAVE >= WANT (strips -rev/+build suffixes).
version_ge() {
    _have="${1%%[-+]*}"; _want="${2%%[-+]*}"
    _s=sort; sort -V </dev/null >/dev/null 2>&1 || { command -v gsort >/dev/null 2>&1 && _s=gsort; }
    [ "$(printf '%s\n%s\n' "$_want" "$_have" | "$_s" -V | head -1)" = "$_want" ]
}

if [ "$CHECK_DEPS" = 1 ] || [ "$DRY_RUN" = 1 ]; then
    _SUDO_DISPLAY="$SUDO"   # remember the real sudo prefix for dry-run printing
    SUDO=":"                # neutralize privileged side-commands (no mutation)
fi

# dry-run colors (only on a real terminal; plain when piped, e.g. CI logs).
if [ "$DRY_RUN" = 1 ] && [ -t 1 ]; then
    _DRY_C="$(printf '\033[0;34m')"; _DRY_H="$(printf '\033[1;36m')"; _DRY_R="$(printf '\033[0m')"
else _DRY_C=""; _DRY_H=""; _DRY_R=""; fi
_dry_line() { printf '%s%s%s\n' "$_DRY_C" "$*" "$_DRY_R"; }
_dry_head() { printf '%s%s%s\n' "$_DRY_H" "$*" "$_DRY_R"; }

# _run CMD... — install: execute (real sudo prefix); dry-run: print it (blue);
# list: skip. Callers pre-filter to missing packages.
_apt_df_shown=0
_run() {
    if [ "$CHECK_DEPS" = 1 ]; then return 0
    elif [ "$DRY_RUN" = 1 ]; then
        case " $* " in
            *' apt-get '*|*' dnf '*|*' yum '*|*' tdnf '*|*' apk '*)
                # First pkg-manager command: on apt, emit the noninteractive
                # frontend right before it — only when there IS an apt command,
                # so a box needing no apt install prints no export noise.
                if [ "$PM" = apt ] && [ "$_apt_df_shown" = 0 ]; then
                    _dry_line 'export DEBIAN_FRONTEND=noninteractive'; _apt_df_shown=1
                fi
                # apt-get/dnf/... read stdin; in a pasted dry-run that stdin IS
                # the following commands, so the pkg manager swallows the rest of
                # the paste. Redirect from /dev/null so a paste runs to the end.
                _dry_line "${_SUDO_DISPLAY:+$_SUDO_DISPLAY }$* < /dev/null" ;;
            *)  _dry_line "${_SUDO_DISPLAY:+$_SUDO_DISPLAY }$*" ;;
        esac
    else ${_SUDO_DISPLAY:-$SUDO} "$@"; fi
}

# _sh 'RAW SHELL' — a mutating step that doesn't fit _run (pipes, redirects,
# &&-chains). dry-run: print it verbatim (keeps output a copy-pasteable script);
# list: skip; else: eval. Build the string with a "$MODE " prefix (expanded at
# call time) so it stays correct on root containers without sudo, exactly as
# the raw scripts did.
_sh() {
    if [ "$DRY_RUN" = 1 ]; then _dry_line "$1"
    elif [ "$CHECK_DEPS" = 1 ]; then return 0
    else eval "$1"; fi
}

# _env 'STR' — environment setup (e.g. `export PATH=…`, guarded `. ~/.cargo/env`)
# that must take effect in EVERY mode (so later `command -v` checks in this
# process see it) AND be shown in dry-run (so a pasted dry-run sets the shell up
# the same way a real bootstrap does). Unlike _sh, it always evaluates.
_env() {
    if [ "$DRY_RUN" = 1 ]; then _dry_line "$1"; fi
    eval "$1"
}

_missing_only() { for _p in "$@"; do _pkg_installed "$_p" || printf '%s ' "$_p"; done; }

_pkg_installed() {
    case "$PM" in
        apt)          dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q 'ok installed' ;;
        dnf|yum|tdnf) rpm -q "$1" >/dev/null 2>&1 ;;
        apk)          apk info -e "$1" >/dev/null 2>&1 ;;
        brew)         [ -n "$(brew list --versions "$1" 2>/dev/null)" ] ;;
        *)            return 1 ;;
    esac
}

_check_pkgs() {
    for _p in "$@"; do
        if _is_optional "$_p"; then
            if _pkg_installed "$_p"; then DEPS_OPT_OK="$DEPS_OPT_OK $_p"; else DEPS_OPT_MISSING="$DEPS_OPT_MISSING $_p"; fi
        else
            _min=$(_min_for "$_p")
            if ! _pkg_installed "$_p"; then
                if [ -n "$_min" ]; then DEPS_MISSING="$DEPS_MISSING $_p:$_min"; else DEPS_MISSING="$DEPS_MISSING $_p"; fi
            elif [ -n "$_min" ]; then
                _have=$(_get_installed_version "$_p")
                if [ -z "$_have" ] || ! version_ge "$_have" "$_min"; then
                    DEPS_MISSING="$DEPS_MISSING $_p:$_min"
                else
                    DEPS_OK="$DEPS_OK $_p"
                fi
            else
                DEPS_OK="$DEPS_OK $_p"
            fi
        fi
    done
}

# ----------------------------------------------------------------------------
# Per-PM install primitives
# ----------------------------------------------------------------------------

# apt-get: mirror apt_get_cmd.sh's DPkg lock timeout; update runs once per
# invocation (matches the raw scripts' single `apt-get update` before install).
APT_GET_LOCK_TIMEOUT_SECONDS="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}"
_pm_apt_updated=0
apt_install() {
    [ "$#" -gt 0 ] || return 0
    if [ "$CHECK_DEPS" = 1 ]; then _check_pkgs "$@"; return 0; fi
    if [ "$DRY_RUN" = 1 ]; then set -- $(_missing_only "$@"); [ "$#" -gt 0 ] || return 0; fi
    export DEBIAN_FRONTEND=noninteractive
    if [ "$_pm_apt_updated" = 0 ]; then
        _run apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" update -qq
        _pm_apt_updated=1
    fi
    _run apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" install -yqq "$@"
}

dnf_install() {
    [ "$#" -gt 0 ] || return 0
    if [ "$CHECK_DEPS" = 1 ]; then _check_pkgs "$@"; return 0; fi
    if [ "$DRY_RUN" = 1 ]; then set -- $(_missing_only "$@"); [ "$#" -gt 0 ] || return 0; fi
    _run dnf install -y --nobest --skip-broken --allowerasing "$@"
}

yum_install() {
    [ "$#" -gt 0 ] || return 0
    if [ "$CHECK_DEPS" = 1 ]; then _check_pkgs "$@"; return 0; fi
    if [ "$DRY_RUN" = 1 ]; then set -- $(_missing_only "$@"); [ "$#" -gt 0 ] || return 0; fi
    _run yum install -y --skip-broken "$@"
}

apk_install() {
    [ "$#" -gt 0 ] || return 0
    if [ "$CHECK_DEPS" = 1 ]; then _check_pkgs "$@"; return 0; fi
    if [ "$DRY_RUN" = 1 ]; then set -- $(_missing_only "$@"); [ "$#" -gt 0 ] || return 0; fi
    _run apk add --no-cache "$@"
}

brew_install() {
    [ "$#" -gt 0 ] || return 0
    if [ "$CHECK_DEPS" = 1 ]; then _check_pkgs "$@"; return 0; fi
    if [ "$DRY_RUN" = 1 ]; then set -- $(_missing_only "$@"); [ "$#" -gt 0 ] || return 0; _run brew install "$@"; return 0; fi
    HOMEBREW_NO_AUTO_UPDATE=1 brew install "$@" || true
}
