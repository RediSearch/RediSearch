#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

activate_venv() {
	echo "copy activation script to shell config"
	if [[ $OS_TYPE == Darwin ]]; then
		# For macOS, add both venv activation and PATH prioritization
		echo "source $PWD/venv/bin/activate" >> ~/.bashrc
		echo "source $PWD/venv/bin/activate" >> ~/.zshrc
		# Also ensure the venv bin directory is first in PATH
		echo "export PATH=\"$PWD/venv/bin:\$PATH\"" >> ~/.bashrc
		echo "export PATH=\"$PWD/venv/bin:\$PATH\"" >> ~/.zshrc
	else
		echo "source $PWD/venv/bin/activate" >> ~/.bash_profile
		echo "source $PWD/venv/bin/activate" >> ~/.bashrc
		echo "export PATH=\"$PWD/venv/bin:\$PATH\"" >> ~/.bash_profile
		echo "export PATH=\"$PWD/venv/bin:\$PATH\"" >> ~/.bashrc
	fi
}

python3 -m venv venv
activate_venv
source venv/bin/activate

# Ensure python3 symlink exists in virtual environment
if [[ ! -L "venv/bin/python3" && -f "venv/bin/python" ]]; then
    ln -sf python venv/bin/python3
fi

# For the current session, prioritize the virtual environment
export PATH="$PWD/venv/bin:$PATH"
echo "Current PATH prioritizes virtual environment: $PATH"

pip install --upgrade pip
pip install -q --upgrade setuptools
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"

# Verify that python3 now points to the virtual environment
echo "python3 path: $(which python3)"
echo "python3 version: $(python3 --version)"

pip install -q -r tests/pytests/requirements.txt

# List installed packages
pip list
