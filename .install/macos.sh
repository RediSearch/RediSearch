#!/bin/bash

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

if ! which brew &> /dev/null; then
    echo "Brew is not installed. Install from https://brew.sh"
    exit 1
fi

export HOMEBREW_NO_AUTO_UPDATE=1

brew update
brew install coreutils
brew install make
brew install openssl
brew install wget

# Install Python 3.13 specifically to avoid getting 3.14+
if ! brew list python@3.13 &>/dev/null; then
    brew install python@3.13
fi

# Ensure python3 points to 3.13
BREW_PREFIX=$(brew --prefix)
PYTHON_BIN="$BREW_PREFIX/opt/python@3.13/bin"

# Update profiles with Python path
[[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$PYTHON_BIN"
[[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$PYTHON_BIN"

"$(dirname "$0")/install_llvm.sh"

GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin

# Update both profile files with all tools
[[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$GNUBIN" "$COREUTILS"
[[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$GNUBIN" "$COREUTILS"
