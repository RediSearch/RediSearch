#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)

export RUSTUP_HOME=/usr/local/rust
export CARGO_HOME=/usr/local/rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
export PATH=$PATH:/usr/local/rust/bin

# Verify Cargo is in path
echo $PATH

rustup update
rustup update nightly
# for RedisJSON build with address sanitizer
rustup component add rust-src --toolchain nightly
