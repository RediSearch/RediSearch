#!/usr/bin/env bash
set -ex

processor=$(uname -m)
OS_TYPE=$(uname -s)

# Always install to the current user's HOME directory
# In containers: HOME=/root (running as root)
# On GitHub runners: HOME=/home/runner (running as runner user)
export UV_INSTALL_DIR=$HOME/.local/bin

curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$UV_INSTALL_DIR" sh
# Add the newly installed `uv` to the PATH
export PATH="$UV_INSTALL_DIR:$PATH"

# Verify uv is in path
uv -vV
# Print where `uv` is located for debugging purposes
echo "uv binary location: $(which uv)"
