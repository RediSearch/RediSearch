#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

# retrieve nightly version
NIGHTLY_VERSION=$(cat "$(dirname "$0")/../../.rust-nightly")
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

# Install a pinned version of `cargo-binstall`,
# to fetch prebuilt release artefacts for the tools we use
export BINSTALL_VERSION="1.17.7"
curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/4c4aeb61ee54318eba5737b7c07aa509a2ed6d32/install-from-binstall-release.sh | bash
# Tool required to compute test coverage for Rust code
cargo binstall cargo-llvm-cov@0.8.4 -y --locked --strategies="crate-meta-data,compile"
# Our preferred test runner, instead of the default `cargo test`
cargo binstall cargo-nextest@0.9.130 -y --locked --strategies="crate-meta-data,compile"
# Tool to aggressively unify the feature sets of our dependencies,
# thus improving the cacheability of our builds
# See https://docs.rs/cargo-hakari/latest/cargo_hakari/about/
cargo binstall cargo-hakari@0.9.37 -y --locked --strategies="crate-meta-data,compile"
# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +$NIGHTLY_VERSION miri setup
