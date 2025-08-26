#!/usr/bin/env bash
processor=$(uname -m)
OS_TYPE=$(uname -s)

curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | sh

# Verify uv is in path
uv -vV
# Print where `cargo` is located for debugging purposes
echo "uv binary location: $(which uv)"

# Update to the latest version of `uv`, in case it was already installed
uv self update
