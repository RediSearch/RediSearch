#!/bin/bash

OS_TYPE=$(uname -s)

if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@17
else
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    apt install lsb-release wget software-properties-common gnupg
    ./llvm.sh 17
fi
