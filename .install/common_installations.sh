#!/bin/bash
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

# create virtual env
python -m venv venv
source venv/bin/activate

pip install -q --upgrade setuptools
pip install --upgrade pip
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"

pip install -q -r tests/pytests/requirements.txt
if [[ $OS_TYPE = 'Darwin' ]]
then
    pip install -q -r tests/pytests/requirements.macos.txt
else
    pip install -q -r tests/pytests/requirements.linux.txt
fi

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r ./install/build_package_requirments.txt

# List installed packages
pip list

echo "ACTIVATE_PATH=$PWD/venv/bin/activate" >> $GITHUB_ENV
echo "source $ACTIVATE_PATH" >> ~/.bashrc
echo "echo $ACTIVATE_PATH" >> ~/.bashrc
echo "pip: $(which pip)" >> ~/.bashrc
