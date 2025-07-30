#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)

export RUSTUP_HOME=/usr/local/rust
export CARGO_HOME=/usr/local/rust
apt install sudo -y
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo sh -s -- -y --no-modify-path
export PATH=$PATH:/usr/local/rust/bin

# Verify Cargo is in path
echo $PATH

sudo rustup update
sudo rustup update nightly
# for RedisJSON build with address sanitizer
sudo rustup component add rust-src --toolchain nightly
