#!/usr/bin/env bash
set -exo pipefail

processor=$(uname -m)
OS_TYPE=$(uname -s)

# Always install to the current user's HOME directory
# In containers: HOME=/root (running as root)
# On GitHub runners: HOME=/home/runner (running as runner user)
export UV_INSTALL_DIR=$HOME/.local/bin
# Make sure the eventual uv install dir is on PATH for the duration of
# this script, so the `command -v uv` check below sees a pre-installed
# uv even when this shell's PATH didn't already include $HOME/.local/bin.
export PATH="$UV_INSTALL_DIR:$PATH"

# Skip the install if uv is already present. Re-running install_python.sh
# should be a fast no-op, not another network download and tarball extract.
if ! command -v uv >/dev/null 2>&1; then
    echo "Installing uv (no existing uv on PATH)..."
    curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$UV_INSTALL_DIR" sh
else
    echo "uv already installed at $(command -v uv) - skipping installer"
fi

# Verify uv is in path
uv -vV
# Print where `uv` is located for debugging purposes
echo "uv binary location: $(which uv)"
