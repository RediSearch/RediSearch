#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

echo "Installing Python dependencies system-wide"

# Upgrade pip for the system Python
python3 -m pip install --upgrade pip
python3 -m pip install -q --upgrade setuptools
echo "pip version: $(python3 -m pip --version)"
echo "pip path: $(which pip3)"

# Verify Python version and path
echo "python3 path: $(which python3)"
echo "python3 version: $(python3 --version)"

# Install test dependencies system-wide
python3 -m pip install -q -r tests/pytests/requirements.txt

# List installed packages
python3 -m pip list
