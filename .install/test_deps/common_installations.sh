#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

activate_venv() {
  echo "copy ativation script to shell config"
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

    # cargo
    echo '. "$HOME/.cargo/env"' >> ~/.bash_profile
    # uv
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bash_profile
  fi
}

# retrieve nightly version from build.sh
NIGHTLY_VERSION=$(grep "NIGHTLY_VERSION=" build.sh | cut -d'=' -f2 | tr -d '"')
# --allow-downgrade:
#   Allow `rustup` to install an older `nightly` if the latest one
#   is missing one of the components we need.
# llvm-tools-preview:
#   Required by `cargo-llvm-cov` for test coverage
# miri:
#   Required to run `cargo miri test` for UB detection
# rust-src:
#   Required to build RedisJSON with address sanitizer
rustup toolchain install $NIGHTLY_VERSION \
    --allow-downgrade \
    --component llvm-tools-preview \
    --component miri \
    --component rust-src

# Tool required to compute test coverage for Rust code
cargo install cargo-llvm-cov --locked
# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +$NIGHTLY_VERSION miri setup

# Create a virtual environment for Python tests, with `pip` pre-installed (--seed)
uv venv --seed
activate_venv
source .venv/bin/activate
uv sync --locked --all-packages

# List installed packages
uv run pip list
