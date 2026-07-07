#!/usr/bin/env bash
set -eo pipefail
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

		# Always use $HOME - works in both container and non-container
		# In container: HOME=/root (fixed by workflow step 117-132)
		# On runner: HOME=/home/runner (already correct)
		# cargo
		echo '. "$HOME/.cargo/env"' >> ~/.bash_profile
		# rustup
		echo 'export RUSTUP_HOME=$HOME/.rustup' >> ~/.bash_profile
		# uv
		echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bash_profile
	fi
}

# On musl (Alpine), uv's standalone CPython is clang-built and bakes
# clang-only flags (--rtlib=compiler-rt) into its sysconfig, so sdist-only
# deps (psutil, ml-dtypes) fail to compile with the system gcc — build
# them with the clang toolchain the LTO setup already installed instead
# (alpine_linux_3.sh provides compiler-rt for the runtime). Pin the
# interpreter to the newest CPython with musllinux wheels for the heavy
# scientific deps on both arches (scipy ships cp313 musl wheels; the
# distro python may be newer, e.g. 3.14, forcing a scipy source build).
if [[ -f /etc/alpine-release ]]; then
    export CC=clang CXX=clang++
    export UV_PYTHON=3.13
fi

# Create a virtual environment for Python tests, with `pip` pre-installed (--seed).
# --clear ensures a partial .venv left behind by a failed (e.g. network-timed-out)
# attempt is replaced rather than causing "virtual environment already exists" on retry.
uv venv --seed --clear
activate_venv
source .venv/bin/activate
uv sync --locked --all-packages

# List installed packages
uv run pip list
