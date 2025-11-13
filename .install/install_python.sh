#!/usr/bin/env bash
set -ex

processor=$(uname -m)
OS_TYPE=$(uname -s)

if [[ $GITHUB_ACTIONS == "true" ]]; then
	export UV_INSTALL_DIR=/root/.local/bin
else
	export UV_INSTALL_DIR=$HOME/.local/bin
fi

curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="/root/.local/bin" sh
# Add the newly installed `uv` to the PATH
export PATH="$UV_INSTALL_DIR:$PATH"

# Verify uv is in path
uv -vV
# Print where `uv` is located for debugging purposes
echo "uv binary location: $(which uv)"
