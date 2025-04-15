#!/bin/bash

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

OS_TYPE=$(uname -s)
VERSION=18
MODE=$1

if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@$VERSION
    BREW_PREFIX=$(brew --prefix)
    LLVM="$BREW_PREFIX/opt/llvm@$VERSION/bin"

    # Update profiles with LLVM path
    [[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$LLVM"
    [[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$LLVM"
else
    $MODE apt install -y lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh $VERSION
fi
