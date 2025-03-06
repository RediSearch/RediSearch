#!/bin/bash

OS_TYPE=$(uname -s)

VERSION=18
MODE=$1



if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@$VERSION
else
    $MODE apt install -y lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh $VERSION
fi
