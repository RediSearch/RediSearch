#!/usr/bin/env bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

# retrieve nightly version from build.sh
NIGHTLY_VERSION=$(grep "NIGHTLY_VERSION=" build.sh | cut -d'=' -f2 | tr -d '"' | head -n1)
# --allow-downgrade:
#   Allow `rustup` to install an older `nightly` if the latest one
#   is missing one of the components we need.
# llvm-tools-preview:
#   Required by `cargo-llvm-cov` for test coverage
# miri:
#   Required to run `cargo miri test` for UB detection
# rust-src:
#   Required to build RedisJSON with address sanitizer
rustup toolchain install $NIGHTLY_VERSION \
    --allow-downgrade \
    --component llvm-tools-preview \
    --component miri \
    --component rust-src

# Tool required to compute test coverage for Rust code
cargo install cargo-llvm-cov --locked
# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +$NIGHTLY_VERSION miri setup
