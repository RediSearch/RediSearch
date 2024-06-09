#!/bin/bash
processor=$(uname -m)
OS_TYPE=$(uname -s)


curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"  
rustup update 
rustup update nightly
# for RedisJSON build with addess santizer
rustup component add rust-src --toolchain nightly
# if [[ $OS_TYPE = 'Darwin' ]]
# then
#     rustup component add rust-src --toolchain nightly-aarch64-apple-darwin
# else
#     if [[ $processor = 'x86_64' ]]
#     then
#         rustup component add rust-src --toolchain nightly-x86_64-unknown-linux-gnu
#     else
#         rustup component add rust-src --toolchain nightly-aarch64-unknown-linux-gnu
#     fi
# fi

