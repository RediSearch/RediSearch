#!/bin/bash

OS_TYPE=$(uname -s)

MODE=$1 



if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@17
else
    $MODE apt install lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh 17
fi
