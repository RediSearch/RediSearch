#!/usr/bin/env bash
set -exo pipefail

processor=$(uname -m)
OS_TYPE=$(uname -s)
MIN_UV_VERSION=0.9.13

# Always install to the current user's HOME directory
# In containers: HOME=/root (running as root)
# On GitHub runners: HOME=/home/runner (running as runner user)
export UV_INSTALL_DIR=$HOME/.local/bin
# Make sure the eventual uv install dir is on PATH for the duration of
# this script, so the `command -v uv` check below sees a pre-installed
# uv even when this shell's PATH didn't already include $HOME/.local/bin.
export PATH="$UV_INSTALL_DIR:$PATH"

# Reuse an existing uv only if it meets the minimum version required by this
# repo. This keeps bootstrap idempotent without accepting stale uv binaries
# from the system or a previous manual install.
have_ver="$(uv --version 2>/dev/null | awk '/^uv / {print $2; exit}' || true)"
if [[ -n "$have_ver" && "$(printf '%s\n' "$MIN_UV_VERSION" "$have_ver" | sort -V | head -1)" == "$MIN_UV_VERSION" ]]; then
    echo "uv $have_ver already installed (>= required $MIN_UV_VERSION) at $(command -v uv) - skipping installer"
else
    if [[ -n "$have_ver" ]]; then
        echo "uv $have_ver is older than required $MIN_UV_VERSION - installing fresh uv"
    else
        echo "Installing uv (no existing uv on PATH)..."
    fi
    curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$UV_INSTALL_DIR" sh
fi

# Verify uv is in path
uv -vV
# Print where `uv` is located for debugging purposes
echo "uv binary location: $(which uv)"
