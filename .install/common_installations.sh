#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

activate_venv() {
	echo "copy ativation script to shell config"
	if [[ $OS_TYPE == Darwin ]]; then
		echo "source venv/bin/activate" >> ~/.bashrc
		echo "source venv/bin/activate" >> ~/.zshrc
	else
		echo "source $PWD/venv/bin/activate" >> ~/.bash_profile
	fi
}

python3 -m venv venv
activate_venv
source venv/bin/activate

pip install --upgrade pip
pip install -q --upgrade setuptools
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"

pip install -q -r tests/pytests/requirements.txt

# List installed packages
pip list
