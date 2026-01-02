#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Verify Cargo is in path
echo $PATH

rustup update
rustup update nightly

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

