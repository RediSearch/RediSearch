#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Verify Cargo is in path
echo $PATH

rustup update
rustup update nightly
# for RedisJSON build with address sanitizer
rustup component add rust-src --toolchain nightly
