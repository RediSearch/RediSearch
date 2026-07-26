#!/usr/bin/env bash
set -eo pipefail
version=3.25.1
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not
source "$(dirname "${BASH_SOURCE[0]}")/version_compare.sh"

# Skip if a cmake meeting the minimum version is already on PATH.
# Re-running this script should be a no-op when cmake is already present.
have_ver="$(cmake --version 2>/dev/null | awk '/cmake version/ {print $3; exit}' || true)"

# list: record cmake presence (version-checked); dry-run: print the install
# commands only if the required cmake isn't already present; real: unchanged.
if [ "${CHECK_DEPS:-0}" = 1 ]; then
    if [[ -n "$have_ver" ]] && version_ge "$have_ver" "$version"; then DEPS_OK="$DEPS_OK cmake"; else DEPS_MISSING="$DEPS_MISSING cmake:$version"; fi
    return 0 2>/dev/null || exit 0
fi
if [ "${DRY_RUN:-0}" = 1 ]; then
    if [[ -n "$have_ver" ]] && version_ge "$have_ver" "$version"; then
        return 0 2>/dev/null || exit 0
    fi
    if [[ $OS_TYPE = 'Darwin' ]]; then
        _dry_line "brew install cmake"
    else
        OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g'); OS_NAME=${OS_NAME#"NAME="}
        if [[ $OS_NAME == 'Alpine Linux' ]]; then
            _dry_line "${MODE:+$MODE }apk add --no-cache cmake"
        else
            processor=$(uname -m)
            if [[ $processor = 'x86_64' ]]; then filename=cmake-${version}-linux-x86_64.sh; else filename=cmake-${version}-linux-aarch64.sh; fi
            _dry_line "wget https://github.com/Kitware/CMake/releases/download/v${version}/${filename}"
            _dry_line "chmod u+x ./${filename}"
            _dry_line "${MODE:+$MODE }./${filename} --skip-license --prefix=/usr/local --exclude-subdir"
            _dry_line "rm ./${filename}"
        fi
    fi
    return 0 2>/dev/null || exit 0
fi

if [[ -n "$have_ver" ]] && version_ge "$have_ver" "$version"; then
    echo "cmake $have_ver already installed (>= required $version) - skipping"
    return 0 2>/dev/null || exit 0
elif [[ -n "$have_ver" ]]; then
    echo "cmake $have_ver is older than required $version - upgrading"
fi

if [[ $OS_TYPE = 'Darwin' ]]
then
    brew install cmake
else
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    if [[ $OS_NAME == 'Alpine Linux' ]]
    then
        $MODE apk add --no-cache cmake
    else
        processor=$(uname -m)
        if [[ $processor = 'x86_64' ]]
        then
            filename=cmake-${version}-linux-x86_64.sh
        else
            filename=cmake-${version}-linux-aarch64.sh
        fi

        wget https://github.com/Kitware/CMake/releases/download/v${version}/${filename}
        chmod u+x ./${filename}
        $MODE ./${filename} --skip-license --prefix=/usr/local --exclude-subdir
        cmake --version
        rm ./${filename}
    fi
fi
