#!/usr/bin/env bash
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
		# Adding the virtual environment activation script to the shell profile
		# causes $PATH issues on platforms like Debian and Alpine,
		# shadowing the pre-existing source command to make some of our tools available.
		# We work around it by appending the required lines to the shell profile
		# _after_ the venv activation script

		if [[ $GITHUB_ACTIONS == "true" ]]; then
		    # cargo
			echo '. "/root/.cargo/env"' >> ~/.bash_profile
            # rustup
			echo 'export RUSTUP_HOME=/root/.rustup' >> ~/.bash_profile
			# uv
			echo 'export PATH="/root/.local/bin:$PATH"' >> ~/.bash_profile
		else
            # cargo
			echo '. "$HOME/.cargo/env"' >> ~/.bash_profile
			# uv
			echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bash_profile
		fi
	fi
}

# Create a virtual environment for Python tests, with `pip` pre-installed (--seed)
uv venv --seed
activate_venv
source .venv/bin/activate
uv sync --locked --all-packages

# List installed packages
uv run pip list
