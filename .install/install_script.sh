#!/usr/bin/env bash
set -eo pipefail

OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not
export MODE

HERE="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

if [[ $OS_TYPE = 'Darwin' ]]
then
    OS='macos'
else
    VERSION=$(grep '^VERSION_ID=' /etc/os-release | sed 's/"//g')
    VERSION=${VERSION#"VERSION_ID="}
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    # AlmaLinux and RHEL are compatible with Rocky Linux install scripts.
    [[ $OS_NAME == 'AlmaLinux' ]] && OS_NAME='Rocky Linux'
    [[ $OS_NAME == 'Red Hat Enterprise Linux' ]] && OS_NAME='Rocky Linux'
    [[ $OS_NAME == 'Rocky Linux' ]] && VERSION=${VERSION%.*} # remove minor version for Rocky Linux
    [[ $OS_NAME == 'Alpine Linux' ]] && VERSION=${VERSION%.*.*} # remove minor and patch version for Alpine Linux
    OS=${OS_NAME,,}_${VERSION}
    OS=$(echo $OS | sed 's/[/ ]/_/g') # replace spaces and slashes with underscores
fi
echo $OS

# Dependency-check / dry-run machinery + package-manager primitives. Sourced
# here (before the OS script) so the OS/source-build scripts can record into
# DEPS_* in list mode and print (nothing installed) in dry-run mode.
source "$HERE/deps_lib.sh"

echo "==> [redisearch] OS=$OS PM=$PM"

source ${OS}.sh $MODE
source install_cmake.sh $MODE

# Boost is only useful when the build runs from the same checkout this script
# populates. The CI image builds from /project but jobs build from a fresh
# workspace checkout, so a baked boost is never used (CMake FetchContent
# re-fetches it). Allow the image build to skip it via SKIP_BOOST=1.
if [[ "${SKIP_BOOST:-0}" != 1 ]]; then
    source ./install_boost.sh
fi
# Install Rust and Python here since they're needed on all platforms and
# the installer doesn't rely on any platform-specific tools (e.g. the package manager)
source install_rust.sh
source install_python.sh

# Python test deps (pip-capable venv + RLTest, via uv) live in test_deps/,
# which CI runs as a separate step; run it here too so a plain
# `make bootstrap` also covers the pytest flow. Runs from the repo root, where
# the uv project lives. install_python_deps.sh is CHECK_DEPS/DRY_RUN aware, so
# it records/prints (nothing installed) in those modes.
if [[ "${SKIP_PYTHON_TEST_DEPS:-0}" != 1 ]]; then
    (cd "$ROOT" && \
        SKIP_VENV_PROFILE_ACTIVATION=1 \
        bash .install/test_deps/install_python_deps.sh $MODE)
fi

# Allow git operations on the checked-out source even when its uid doesn't
# match the current user (common in CI containers). Skipped in list/dry-run
# mode — neither may mutate anything.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then
    git config --global --add safe.directory '*'
fi

if [ "${DRY_RUN:-0}" = 1 ]; then
    _dry_head "==> [redisearch] dry-run complete — commands above are what bootstrap would run for missing deps (nothing installed)"
    exit 0
fi

if [ "${CHECK_DEPS:-0}" = 1 ]; then
    n_ok=$(set -- $DEPS_OK; echo $#)
    n_missing=$(set -- $DEPS_MISSING; echo $#)
    total=$((n_ok + n_missing))
    # Aggregate mode: when the outer bootstrap sets DEPS_REPORT_FILE, don't
    # print a per-module list — append machine-readable records and let the
    # caller print one deduped union across all modules.
    if [ -n "${DEPS_REPORT_FILE:-}" ]; then
        for _p in $DEPS_OK;           do echo "ok $_p"           >> "$DEPS_REPORT_FILE"; done
        for _p in $DEPS_MISSING;      do echo "missing $_p"      >> "$DEPS_REPORT_FILE"; done
        for _p in $DEPS_OPT_OK;       do echo "opt_ok $_p"       >> "$DEPS_REPORT_FILE"; done
        for _p in $DEPS_OPT_MISSING;  do echo "opt_missing $_p"  >> "$DEPS_REPORT_FILE"; done
        echo "==> [redisearch] checked: $n_ok installed, $n_missing missing"
        exit 0
    fi
    echo
    echo "==> [redisearch] dependency check (OS=$OS, PM=$PM) — nothing was installed"
    if [ -t 1 ]; then RED="$(printf '\033[1;31m')"; GRN="$(printf '\033[1;32m')"; RST="$(printf '\033[0m')"; else RED=""; GRN=""; RST=""; fi
    if [ -n "$DEPS_MISSING" ]; then
        echo "${RED}NOT INSTALLED ($n_missing):${RST}"
        for _p in $DEPS_MISSING; do
            case "$_p" in *:*) echo "${RED}    ${_p%%:*} (>= ${_p#*:})${RST}" ;; *) echo "${RED}    $_p${RST}" ;; esac
        done
    else
        echo "${GRN}not installed: (none)${RST}"
    fi
    if [ "${VERBOSE:-0}" = 1 ]; then
        echo "${GRN}installed:${RST}"
        for _p in $DEPS_OK; do echo "${GRN}    $_p${RST}"; done
        [ -n "$DEPS_OK" ] || echo "    (none)"
    else
        echo "${GRN}installed: $n_ok/$total (set VERBOSE=1 to list)${RST}"
    fi
    if [ -n "$DEPS_OPT_MISSING" ]; then
        echo "optional, not installed (tests/coverage/debug only):"
        for _p in $DEPS_OPT_MISSING; do echo "    $_p"; done
    fi
    [ "$n_missing" -eq 0 ] || exit 1
    exit 0
fi

echo "==> [redisearch] install_script.sh: done"
