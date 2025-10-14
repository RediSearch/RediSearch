#!/usr/bin/env bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Print where `rustup` is located for debugging purposes
echo "Rustup binary location: $(which rustup)"
# Verify Cargo is in path
cargo -vV
# Print where `cargo` is located for debugging purposes
echo "Cargo binary location: $(which cargo)"

# Update to the latest stable toolchain
rustup update
