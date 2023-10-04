#!/bin/bash
version=3.25.1
processor=$(uname -p)
OS_TYPE=$(uname -s)

if [[ $OS_TYPE = 'Darwin' ]]
then
    brew install cmake
else
    if [[ $processor = 'x86_64' ]]
    then
        filename=cmake-${version}-linux-x86_64.sh
    else
        filename=cmake-${version}-linux-aarch64.sh
    fi

    wget https://github.com/Kitware/CMake/releases/download/v${version}/${filename}
    chmod u+x ./${filename}
    ./${filename} --skip-license --prefix=/usr/local --exclude-subdir
    cmake --version
fi
