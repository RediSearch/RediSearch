#!/bin/bash

# Function to update shell profile with necessary paths
update_profile() {
    local profile_file=$1
    shift
    local paths=("$@")

    echo "Updating $profile_file with PATH additions"

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
