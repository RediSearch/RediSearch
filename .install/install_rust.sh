#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Verify Cargo is in path
cargo -vV
# Print where `cargo` is located for debugging purposes
echo "Cargo binary location: $(which cargo)"

# Update to the latest stable toolchain
rustup update
# Include `rust-src` component to build RedisJSON with address sanitizer
rustup toolchain install nightly -c rust-src
