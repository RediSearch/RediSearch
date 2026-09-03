#!/usr/bin/env bash
set -eo pipefail
version=3.25.1
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

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

        # -o truncates, so a retried bootstrap overwrites a partial download
        # instead of executing a stale one. --proto/--proto-redir hold the
        # transfer on HTTPS across GitHub's redirect; the file is executed
        # below, so an http:// downgrade would be code execution.
        curl -fsSL --proto '=https' --proto-redir '=https' \
             -o ${filename} \
             https://github.com/Kitware/CMake/releases/download/v${version}/${filename}
        chmod u+x ./${filename}
        $MODE ./${filename} --skip-license --prefix=/usr/local --exclude-subdir
        cmake --version
        rm ./${filename}
    fi
fi
