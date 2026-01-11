#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

activate_venv() {
  echo "copy activation script to shell config"
  if [[ $OS_TYPE == Darwin ]]; then
    echo "source .venv/bin/activate" >> ~/.bashrc
    echo "source .venv/bin/activate" >> ~/.zshrc
  else
    echo "source $PWD/.venv/bin/activate" >> ~/.bash_profile
    echo "source $PWD/.venv/bin/activate" >> ~/.bashrc
  fi
}

# Create a virtual environment for Python tests, with `pip` pre-installed (--seed)
uv venv --seed
activate_venv
source .venv/bin/activate
uv sync --locked --all-packages

# List installed packages
uv run pip list
