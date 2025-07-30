#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)

apt install sudo -y
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y
source /root/.cargo/env

# Verify Cargo is in path
echo $PATH

rustup update
rustup update nightly
# for RedisJSON build with address sanitizer
rustup component add rust-src --toolchain nightly
