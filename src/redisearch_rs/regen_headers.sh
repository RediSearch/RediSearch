#!/usr/bin/env bash
#
# Single source of truth for the cheadergen invocation. Called by:
#   - the top-level Makefile's `generate-rust-headers` target
#   - the `cheadergen_generate` CMake custom target in this directory's
#     CMakeLists.txt
#
# `env -u CARGO_BUILD_TARGET RUSTFLAGS=...` scrubs parent-build env so flags
# set for the main Rust build don't leak into cheadergen's internal
# `cargo metadata` / `cargo doc` invocations:
#   - RUSTFLAGS may contain nightly-only flags (e.g. `-Zsanitizer=address`
#     under `SAN=address`) that stable cargo metadata rejects. Empty
#     `RUSTFLAGS=` is fine — cargo treats it as "no flags".
#   - CARGO_BUILD_TARGET is set explicitly under `SAN=address`. If it
#     leaks, cargo writes rustdoc JSON to `$TARGET_DIR/<triple>/doc/` while
#     cheadergen reads from `$TARGET_DIR/doc/`, producing a "No such file
#     or directory" failure. Empty `CARGO_BUILD_TARGET=` is NOT ok — cargo
#     aborts with "error: target was empty". Must be truly unset; POSIX
#     `env -u` does that on this Unix-only build.
#
# We do, however, propagate `RUST_DYN_CRT=1` (set by `build.sh` on Alpine
# musl): without `-C target-feature=-crt-static`, build-script crates that
# pull in `bindgen` (and therefore libclang/libLLVM/libncursesw/libz) try
# to link statically and fail because the static versions of those system
# libraries are not installed in the Alpine CI image.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

# Verify that `cheadergen` is installed and matches the version pinned in
# .cheadergen-version (the same file `.install/test_deps/install_rust_deps.sh`
# uses when installing the tool). Running with a different version would
# silently produce headers that disagree with what CI generates.
EXPECTED_CHEADERGEN_VERSION=$(cat "$REPO_ROOT/.cheadergen-version")
if ! command -v cheadergen >/dev/null 2>&1; then
    echo "error: cheadergen is not installed or not on PATH." >&2
    echo "       Install the pinned version with:" >&2
    echo "         cargo install --locked cheadergen_cli@${EXPECTED_CHEADERGEN_VERSION}" >&2
    echo "       or re-run .install/test_deps/install_rust_deps.sh." >&2
    exit 1
fi
# `cheadergen --version` prints `cheadergen_cli <version>`; grab the last field.
ACTUAL_CHEADERGEN_VERSION=$(cheadergen --version | awk '{print $NF}')
if [[ "$ACTUAL_CHEADERGEN_VERSION" != "$EXPECTED_CHEADERGEN_VERSION" ]]; then
    echo "error: cheadergen version mismatch." >&2
    echo "       expected: ${EXPECTED_CHEADERGEN_VERSION} (from .cheadergen-version)" >&2
    echo "       found:    ${ACTUAL_CHEADERGEN_VERSION}" >&2
    echo "       Install the pinned version with:" >&2
    echo "         cargo install --locked cheadergen_cli@${EXPECTED_CHEADERGEN_VERSION}" >&2
    echo "       or re-run .install/test_deps/install_rust_deps.sh." >&2
    exit 1
fi

rustflags=
if [[ "${RUST_DYN_CRT:-}" == "1" ]]; then
    rustflags="-C target-feature=-crt-static"
fi

RUST_TOOLCHAIN=$(cat "$REPO_ROOT/.rust-nightly")

# List the target for C header generations:
# - All crates in the c_entrypoint/ folder (with a few excludes)
# - All crates that use explicit `#[cheadergen::config(export, ..)] annotations
#   to export types that aren't otherwise reachable from the FFI functions
#   we expose.
exec env -u CARGO_BUILD_TARGET RUSTFLAGS="${rustflags}" cheadergen generate \
    --lang c \
    --rust-toolchain="${RUST_TOOLCHAIN}" \
    --prune-orphans \
    --skip-empty \
    --exclude=redisearch_rs \
    --exclude=c_ffi_utils \
    --package=document \
    --package=field \
    --package=index_result \
    --package=query_term \
    --package=inverted_index \
    --package=rlookup \
    --package=rqe_core \
    --package=rqe_iterator_type \
    --package=rqe_iterators \
    --package=search_result \
    --package=query_flags \
    --package=query_types \
    --package=query_eval \
    --config=cheadergen.toml \
    --output-dir=headers \
    --input-dir=c_entrypoint
