#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

echo "Installing Python dependencies system-wide"

# Check if pip is available, if not try to make it work
if ! python3 -m pip --version &>/dev/null; then
    echo "pip not found, trying to bootstrap it..."
    if [[ $OS_TYPE == Darwin ]]; then
        # On macOS, try to install pip via brew first
        echo "Installing pip via brew..."
        if command -v brew &>/dev/null; then
            brew install python-pip || true
        fi

        # Try again after brew install
        if python3 -m pip --version &>/dev/null; then
            echo "pip is now available via python3 -m pip"
            pip_cmd() {
                python3 -m pip "$@" --break-system-packages
            }
        else
            echo "pip still not available, this Python installation may be incomplete"
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
