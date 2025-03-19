#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

activate_venv() {
	echo "copy activation script to shell config"
	if [[ $OS_TYPE == Darwin ]]; then
		echo "source venv/bin/activate" >> ~/.bashrc
		echo "source venv/bin/activate" >> ~/.zshrc
	else
		echo "source $PWD/venv/bin/activate" >> ~/.bash_profile
		echo "source $PWD/venv/bin/activate" >> ~/.bashrc
		# Adding the virtual environment activation script to the shell profile
		# causes $PATH issues on platforms like Debian and Alpine,
		# shadowing the pre-existing source command to make `cargo` available.
		# We work around it by appending the required lines to the shell profile
		# _after_ the venv activation script
		echo '. "$HOME/.cargo/env"' >> ~/.bash_profile
	fi
}

# Tool required to compute test coverage for Rust code
cargo install cargo-llvm-cov --locked

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
