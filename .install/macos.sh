#!/usr/bin/env bash
set -eo pipefail
# Keep the xtrace for real installs, but not in list/dry-run — this file is
# `source`d, so `set -x` would otherwise trace the whole run and bury the
# copy-pasteable dry-run script in noise.
[[ "${CHECK_DEPS:-0}" == 1 || "${DRY_RUN:-0}" == 1 ]] || set -x

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

if ! command -v brew &> /dev/null; then
    if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
        DEPS_MISSING="$DEPS_MISSING brew"
        return 0 2>/dev/null || exit 0
    fi
    if [[ "${DRY_RUN:-0}" == 1 ]]; then
        _dry_line '# Install Homebrew from https://brew.sh before running the brew commands below.'
    else
        echo "Homebrew is not installed. Install from https://brew.sh"
        exit 1
    fi
fi

export HOMEBREW_NO_AUTO_UPDATE=1

_run brew update
brew_install coreutils
brew_install make
brew_install openssl
brew_install wget
# Source (not subprocess) so its list/dry-run DEPS_* records reach the parent
# install_script.sh — a subprocess's records would vanish on exit, dropping
# clang/llvm from `make bootstrap list` on macOS.
source "$(dirname "$0")/install_llvm.sh"

# Profile edits mutate the user's shell config — skip them in list/dry-run mode.
if [[ "${CHECK_DEPS:-0}" != 1 && "${DRY_RUN:-0}" != 1 ]]; then
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
