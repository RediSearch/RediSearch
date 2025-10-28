#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

echo "Installing Python dependencies system-wide"

# Upgrade pip for the system Python (with --break-system-packages for macOS Homebrew Python)
python3 -m pip install --upgrade pip --break-system-packages
python3 -m pip install -q --upgrade setuptools --break-system-packages
echo "pip version: $(python3 -m pip --version)"
echo "pip path: $(which pip3)"

# Verify Python version and path
echo "python3 path: $(which python3)"
echo "python3 version: $(python3 --version)"

# Install test dependencies system-wide (with --break-system-packages for macOS Homebrew Python)
python3 -m pip install -q -r tests/pytests/requirements.txt --break-system-packages

# List installed packages
python3 -m pip list
