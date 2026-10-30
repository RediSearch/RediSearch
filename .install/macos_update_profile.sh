#!/usr/bin/env bash
set -eo pipefail

# Function to update shell profile with necessary paths
update_profile() {
    local profile_file=$1
    shift
    local paths=("$@")

    echo "Updating $profile_file with PATH additions: ${paths[*]}"

    # Check if the profile exists
    if [[ ! -f $profile_file ]]; then
        touch "$profile_file"
    fi

    # Add each path to the profile if not already present
    for path in "${paths[@]}"; do
        if ! grep -q "export PATH=\"$path:\$PATH\"" "$profile_file"; then
            echo "export PATH=\"$path:\$PATH\"" >> "$profile_file"
        fi
    done
}

# Keep the upstream LLVM SDK and PATH together in a startup file the user's
# shell reads. An explicit directory also supports isolated bootstrap checks.
update_llvm_profile() {
    local bindir="$1"
    local user_shell="${2:-${SHELL:-/bin/bash}}"
    local profile_dir="${3:-$HOME}"
    local profile candidate
    case "${user_shell##*/}" in
        bash)
            profile="${profile_dir}/.bash_profile"
            # Bash reads only the first existing login file in this order.
            for candidate in .bash_profile .bash_login .profile; do
                if [[ -f "${profile_dir}/${candidate}" ]]; then
                    profile="${profile_dir}/${candidate}"
                    break
                fi
            done
            ;;
        zsh)
            profile_dir="${3:-${ZDOTDIR:-$HOME}}"
            profile="${profile_dir}/.zshrc"
            if [[ ! -f "$profile" && -f "${profile_dir}/.zprofile" ]]; then
                profile="${profile_dir}/.zprofile"
            fi
            ;;
        *)
            echo "WARNING: configure PATH and SDKROOT for ${user_shell} manually" >&2
            return 0
            ;;
    esac
    mkdir -p "$profile_dir"
    update_profile "$profile" "$bindir"
    # Resolve at shell start so the SDK follows later Xcode updates.
    local sdk_line='export SDKROOT="$(xcrun --sdk macosx --show-sdk-path)"'
    if ! grep -qF "$sdk_line" "$profile"; then
        echo "$sdk_line" >> "$profile"
    fi
}
