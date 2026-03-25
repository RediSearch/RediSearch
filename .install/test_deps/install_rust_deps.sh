#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
processor=$(uname -m)
MODE=$1 # whether to install using sudo or not

# retrieve nightly version
NIGHTLY_VERSION=$(cat "$(dirname "${BASH_SOURCE[0]}")/../../.rust-nightly")
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

# Wrapper around `cargo binstall` that auto-confirms (-y), respects
# the lockfile (--locked), and tries prebuilt binaries first, falling
# back to compiling from source (--strategies).
binstall() {
    cargo binstall "$@" -y --locked --strategies="crate-meta-data,compile"
}

# Tool required to compute test coverage for Rust code
binstall cargo-llvm-cov@0.8.4
# Our preferred test runner, instead of the default `cargo test`
# Use musl targets on Linux for maximum compatibility across glibc versions
# (default builds dynamically against system glibc which causes issues on older systems)
NEXTEST_ARGS=()
if [[ "$OS_TYPE" = "Linux" ]] && [[ "$processor" =~ ^(x86_64|aarch64)$ ]]; then
    NEXTEST_ARGS+=(--target="${processor}-unknown-linux-musl")
fi
binstall "${NEXTEST_ARGS[@]}" cargo-nextest@0.9.130
# Tool to aggressively unify the feature sets of our dependencies,
# thus improving the cacheability of our builds
# See https://docs.rs/cargo-hakari/latest/cargo_hakari/about/
binstall cargo-hakari@0.9.37
# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +$NIGHTLY_VERSION miri setup
