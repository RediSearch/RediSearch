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
		echo "source $PWD/venv/bin/activate" >> ~/.bashrc
	fi
}

# Pin a specific working version of nightly to prevent breaking the CI because
# regressions in a nightly build.
# Make sure to synchronize updates across all modules: Redis and RedisJSON.
NIGHTLY_VERSION="nightly-2025-07-30"

# --allow-downgrade:
#   Allow `rustup` to install an older `nightly` if the latest one
#   is missing one of the components we need.
# rust-src:
#   Required to build RedisJSON with address sanitizer
rustup toolchain install $NIGHTLY_VERSION \
    --allow-downgrade \
    --component rust-src

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
