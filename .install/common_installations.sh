#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

pip install --upgrade pip
pip install -q --upgrade setuptools
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"

pip install -q -r tests/pytests/requirements.txt
# if [[ $OS_TYPE = 'Darwin' ]]
# then
#     pip install -q -r tests/pytests/requirements.macos.txt
# else
#     pip install -q -r tests/pytests/requirements.linux.txt
# fi

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r .install/build_package_requirments.txt

# List installed packages
pip list

# add actiavte venv to github env
echo ACTIVATE_VENV=$PWD/venv/bin/activate >> $GITHUB_ENV
