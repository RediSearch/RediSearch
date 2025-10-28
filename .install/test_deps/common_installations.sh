#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

echo "Installing Python dependencies system-wide"

# Check if pip is available, if not try to make it work
if ! python3 -m pip --version &>/dev/null; then
    echo "pip not found, trying to bootstrap it..."
    if [[ $OS_TYPE == Darwin ]]; then
        # On macOS with Homebrew Python, pip might be available but blocked
        # Try to use the pip3 command directly if it exists
        if command -v pip3 &>/dev/null; then
            echo "Found pip3 command, using that instead"
            # Create a wrapper function to use pip3 instead of python3 -m pip
            pip_cmd() {
                pip3 "$@" --break-system-packages
            }
        else
            echo "No pip found, this Python installation may be incomplete"
            exit 1
        fi
    fi
else
    # pip module is available, use it normally
    pip_cmd() {
        python3 -m pip "$@" --break-system-packages
    }
fi

# Upgrade pip for the system Python
pip_cmd install --upgrade pip
pip_cmd install -q --upgrade setuptools

# Show pip information
if command -v pip3 &>/dev/null; then
    echo "pip version: $(pip3 --version)"
    echo "pip path: $(which pip3)"
else
    echo "pip version: $(python3 -m pip --version)"
fi

# Verify Python version and path
echo "python3 path: $(which python3)"
echo "python3 version: $(python3 --version)"

# Install test dependencies
pip_cmd install -q -r tests/pytests/requirements.txt

# List installed packages
if command -v pip3 &>/dev/null; then
    pip3 list
else
    python3 -m pip list
fi
