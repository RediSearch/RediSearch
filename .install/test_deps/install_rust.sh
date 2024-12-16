#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
export PATH="$HOME/.cargo/bin:$PATH"

rustup update
rustup update nightly
# for RedisJSON build with address sanitizer
rustup component add rust-src --toolchain nightly
