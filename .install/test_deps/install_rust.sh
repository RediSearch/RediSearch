#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"  
rustup update 
rustup update nightly
# for RedisJSON build with addess santizer
rustup component add rust-src --toolchain nightly
