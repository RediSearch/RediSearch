#!/usr/bin/env bash
set -ex

processor=$(uname -m)
OS_TYPE=$(uname -s)

curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env
# Add the newly installed `uv` to the PATH
export PATH="$HOME/.local/bin:$PATH"

# Verify uv is in path
uv -vV
# Print where `cargo` is located for debugging purposes
echo "uv binary location: $(which uv)"

# Update to the latest version of `uv`, in case it was already installed
uv self update
