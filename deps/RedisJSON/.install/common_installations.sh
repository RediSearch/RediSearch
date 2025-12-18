#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

pip3 install --upgrade pip
pip3 install -q --upgrade setuptools
echo "pip version: $(pip3 --version)"
echo "pip path: $(which pip3)"

pip3 install -q -r tests/pytest/requirements.txt
# These packages are needed to build the package
pip3 install -q -r .install/build_package_requirements.txt

# List installed packages
pip3 list
