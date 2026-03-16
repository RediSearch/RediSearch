#!/usr/bin/env bash
set -e
export DEBIAN_FRONTEND=noninteractive

# Source the profile update utility
source "$(dirname "$0")/macos_update_profile.sh"

OS_TYPE=$(uname -s)
VERSION=18
MODE=$1
APT_GET_LOCK_TIMEOUT_SECONDS="${APT_GET_LOCK_TIMEOUT_SECONDS:-600}"

apt_get_cmd() {
    $MODE apt-get -o DPkg::Lock::Timeout="$APT_GET_LOCK_TIMEOUT_SECONDS" "$@"
}

if [[ $OS_TYPE == Darwin ]]; then
    brew install llvm@$VERSION
    BREW_PREFIX=$(brew --prefix)
    LLVM="$BREW_PREFIX/opt/llvm@$VERSION/bin"

    # Update profiles with LLVM path
    [[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$LLVM"
    [[ -f ~/.zshrc ]] && update_profile ~/.zshrc "$LLVM"
else
    apt_get_cmd update -qq
    apt_get_cmd install -yqq lsb-release wget software-properties-common gnupg
    wget https://apt.llvm.org/llvm.sh
    chmod +x llvm.sh
    $MODE ./llvm.sh $VERSION
fi
