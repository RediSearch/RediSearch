#!/bin/bash

if ! which brew &> /dev/null; then
    echo "Brew is not installed. Install from https://brew.sh"
    exit 1
fi

export HOMEBREW_NO_AUTO_UPDATE=1

LLVM_VERSION="18"

brew update
brew install coreutils
brew install make
brew install openssl
brew install llvm@$LLVM_VERSION

BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
LLVM="$BREW_PREFIX/opt/llvm@$LLVM_VERSION/bin"
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin

update_profile() {
    local profile_path=$1
    local newpath="export PATH=$COREUTILS:$LLVM:$GNUBIN:\$PATH"
    grep -qxF "$newpath" "$profile_path" || echo "$newpath" >> "$profile_path"
    source $profile_path
}

[[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile
[[ -f ~/.zshrc ]] && update_profile ~/.zshrc

source install_cmake.sh
