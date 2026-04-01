#!/usr/bin/env bash
set -xeo pipefail

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

if ! which brew &> /dev/null; then
    echo "Homebrew is not installed. Install from https://brew.sh"
    exit 1
fi

export HOMEBREW_NO_AUTO_UPDATE=1

brew update
brew install coreutils
brew install make
brew install openssl
brew install perl
brew install wget
"$(dirname "$0")/install_llvm.sh"

BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin

# Update both profile files with all tools
if [[ -f ~/.bash_profile ]]; then
    update_profile ~/.bash_profile "$GNUBIN" "$COREUTILS"
fi
if [[ -f ~/.zshrc ]]; then
    update_profile ~/.zshrc "$GNUBIN" "$COREUTILS"
fi
