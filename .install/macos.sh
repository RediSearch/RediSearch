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
brew install python@3.12
"$(dirname "$0")/install_llvm.sh"

BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin
PYTHON312=$BREW_PREFIX/opt/python@3.12/bin

# Update both profile files with all tools (Python 3.12 first to ensure precedence)
[[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$PYTHON312" "$GNUBIN" "$COREUTILS"
[[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$PYTHON312" "$GNUBIN" "$COREUTILS"

# Ensure python3 points to Python 3.12 by creating symlinks if they don't exist
if [[ ! -L "$PYTHON312/python3" && -f "$PYTHON312/python3.12" ]]; then
    ln -sf "$PYTHON312/python3.12" "$PYTHON312/python3"
fi
if [[ ! -L "$PYTHON312/pip3" && -f "$PYTHON312/pip3.12" ]]; then
    ln -sf "$PYTHON312/pip3.12" "$PYTHON312/pip3"
fi
