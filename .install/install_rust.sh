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

# --allow-downgrade:
#   Allow `rustup` to install an older `nightly` if the latest one
#   is missing one of the components we need.
# llvm-tools-preview:
#   Required by `cargo-llvm-cov` for test coverage
# miri:
#   Required to run `cargo miri test` for UB detection
# rust-src:
#   Required to build RedisJSON with address sanitizer
rustup toolchain install nightly \
    --allow-downgrade \
    --component llvm-tools-preview \
    --component miri \
    --component rust-src
