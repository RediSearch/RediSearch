#!/bin/bash

MODE=$1 # whether to install using sudo or not

# Download and install rustup
$MODE curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Source the cargo environment script to update the PATH
echo "source $HOME/.cargo/env" >> $HOME/.bashrc
source $HOME/.cargo/env

# Update rustup
$MODE rustup update

# Install the toolchain specified in rust-toolchain.toml (if present)
if [ -f "rust-toolchain.toml" ]; then
    TOOLCHAIN=$(grep -E '^\s*channel\s*=' rust-toolchain.toml | sed 's/.*=\s*"\([^"]*\)".*/\1/' | tr -d ' ')
    if [ -n "$TOOLCHAIN" ]; then
        $MODE rustup toolchain install "$TOOLCHAIN"
    else
        $MODE rustup update nightly
    fi
else
    $MODE rustup update nightly
fi

# Install required components for the active toolchain
$MODE rustup component add rust-src
$MODE rustup component add rustfmt
$MODE rustup component add clippy

# Verify cargo installation
cargo --version

rustup show
