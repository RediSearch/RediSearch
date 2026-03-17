#!/usr/bin/env bash

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

OS_TYPE=$(uname -s)
# Source $LLVM_VERSION
source "$(dirname "$0")/LLVM_VERSION.sh"
MODE=$1

if [[ $OS_TYPE == Darwin ]]; then
    # We use an older LLVM on MacOS as some deps (fmt) do not build with LLVM 21+.
    # (And LTO is currently disabled on MacOS anyway.)
    LLVM_VERSION=20
    brew install llvm@$LLVM_VERSION
    BREW_PREFIX=$(brew --prefix)
    LLVM="$BREW_PREFIX/opt/llvm@$LLVM_VERSION/bin"

    # Update profiles with LLVM path
    [[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$LLVM"
    [[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$LLVM"
else
    $MODE apt install -y lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh $LLVM_VERSION
fi
