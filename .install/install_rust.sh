#!/usr/bin/env bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


if [[ $GITHUB_ACTIONS == "true" ]]; then
	export RUSTUP_HOME=/root/.rustup
	export CARGO_HOME=/root/.cargo
else
    export RUSTUP_HOME=$HOME/.rustup
    export CARGO_HOME=$HOME/.cargo
fi

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $CARGO_HOME/env

# Print where `rustup` is located for debugging purposes
echo "Rustup binary location: $(which rustup)"
# Verify Cargo is in path
cargo -vV
# Print where `cargo` is located for debugging purposes
echo "Cargo binary location: $(which cargo)"

# Update to the latest stable toolchain
rustup update
