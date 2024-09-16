#!/bin/bash

OS_TYPE=$(uname -s)

VERSION=$1
MODE=$2



if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@$VERSION
else
    $MODE apt install -y lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh $VERSION
fi
