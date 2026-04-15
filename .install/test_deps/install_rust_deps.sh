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

# Pick a preferred prebuilt target. On Linux x86_64/aarch64 we prefer
# the musl prebuilt so the binary is statically linked against musl and
# works across a wide range of glibc versions (the default host-target
# prebuilt is dynamically linked against system glibc, which causes
# issues on older systems). Empty on other platforms.
PREFERRED_TARGET=""
if [[ "$OS_TYPE" = "Linux" ]] && [[ "$processor" =~ ^(x86_64|aarch64)$ ]]; then
    PREFERRED_TARGET="${processor}-unknown-linux-musl"
fi

# Check if a cargo tool is already installed at the expected version.
# Used to skip redundant binstall calls when ~/.cargo/bin/ is restored
# from CI cache (Swatinem/rust-cache), saving ~18 min on macOS Intel.
is_installed() {
    local tool="$1" version="$2"
    local bin="${tool##cargo-}" # e.g. cargo-nextest -> nextest
    # Some tools use the full name (cargo-llvm-cov), others use the
    # short name (nextest, hakari) for their --version output.
    if command -v "cargo-$bin" &>/dev/null; then
        local installed
        installed=$("cargo-$bin" --version 2>/dev/null || true)
        if [[ "$installed" == *"$version"* ]]; then
            echo "$tool@$version is already installed, skipping" >&2
            return 0
        fi
    fi
    return 1
}

# Wrapper around `cargo binstall` that auto-confirms (-y) and respects
# the lockfile (--locked).
#
# First tries the $PREFERRED_TARGET prebuilt (when set, e.g. musl on
# Linux for glibc independence). If that fails, falls back to installing
# for the host target — by default trying a host-target prebuilt and
# then compiling from source with the host toolchain. Without the
# host-target fallback we'd fail on hosts that don't have the musl
# cross-toolchain installed when the musl prebuilt is unavailable.
#
# Options (consumed by the wrapper, not forwarded to cargo-binstall):
#   --no-host-prebuilt   Don't accept a host-target prebuilt in the
#                        fallback. Use this for tools where the musl
#                        prebuilt was chosen for glibc compatibility and
#                        a glibc-linked host prebuilt would defeat the
#                        purpose — fall back directly to a source build
#                        with the host toolchain.
binstall() {
    local allow_host_prebuilt=1
    if [[ "$1" == "--no-host-prebuilt" ]]; then
        allow_host_prebuilt=0
        shift
    fi

    # Skip installation if the tool is already present at the right version
    # (e.g. restored from CI cache).
    local spec="$1" # e.g. "cargo-nextest@0.9.130"
    local tool="${spec%@*}"
    local version="${spec#*@}"
    if is_installed "$tool" "$version"; then
        return 0
    fi

    if [[ -n "$PREFERRED_TARGET" ]]; then
        if cargo binstall "$@" -y --locked --force \
                --target="$PREFERRED_TARGET" \
                --strategies=crate-meta-data; then
            return 0
        fi
        echo "Prebuilt $PREFERRED_TARGET artifact unavailable for $*; falling back to host target" >&2
    fi

    # --no-host-prebuilt only matters when a preferred target was set:
    # it guards against falling back to a (glibc-linked) host prebuilt
    # after the preferred (musl) prebuilt failed. On platforms where no
    # preferred target applies (e.g. macOS), there's no such concern and
    # a host-target prebuilt is perfectly fine.
    local strategies="crate-meta-data,compile"
    if (( ! allow_host_prebuilt )) && [[ -n "$PREFERRED_TARGET" ]]; then
        strategies="compile"
    fi
    cargo binstall "$@" -y --locked --force --strategies="$strategies"
}

# Tool required to compute test coverage for Rust code
binstall cargo-llvm-cov@0.8.4
# Our preferred test runner, instead of the default `cargo test`.
# The musl prebuilt is chosen for glibc independence; a host-target
# prebuilt would be glibc-linked and defeat the purpose, so on fallback
# we go straight to a source build with the host toolchain.
binstall --no-host-prebuilt cargo-nextest@0.9.130
# Tool to aggressively unify the feature sets of our dependencies,
# thus improving the cacheability of our builds
# See https://docs.rs/cargo-hakari/latest/cargo_hakari/about/
binstall cargo-hakari@0.9.37
# Make sure `miri` is fully operational before running tests with it.
# See https://github.com/rust-lang/miri/blob/master/README.md#running-miri-on-ci
# for more details.
cargo +$NIGHTLY_VERSION miri setup
