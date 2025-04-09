#!/bin/bash
version=4.0.0
processor=$(uname -m)
OS_TYPE=$(uname -s)
OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
OS_NAME=${OS_NAME#"NAME="}
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    brew install cmake
else
    if [[ $OS_NAME == 'Alpine Linux' ]]
    then
        $MODE apk add --no-cache cmake
    else
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
