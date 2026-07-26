#!/usr/bin/env bash
set -eo pipefail
# Keep the xtrace for real installs, but not in list/dry-run — this file is
# `source`d, so `set -x` would otherwise trace the whole run and bury the
# copy-pasteable dry-run script in noise.
[ "${CHECK_DEPS:-0}" = 1 ] || [ "${DRY_RUN:-0}" = 1 ] || set -x

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

if ! which brew &> /dev/null; then
    echo "Homebrew is not installed. Install from https://brew.sh"
    exit 1
fi

export HOMEBREW_NO_AUTO_UPDATE=1

_run brew update
brew_install coreutils
brew_install make
brew_install openssl
brew_install wget
"$(dirname "$0")/install_llvm.sh"

# Profile edits mutate the user's shell config — skip them in list/dry-run mode.
if [ "${CHECK_DEPS:-0}" != 1 ] && [ "${DRY_RUN:-0}" != 1 ]; then
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
fi
