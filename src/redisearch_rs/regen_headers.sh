#!/usr/bin/env bash
#
# Single source of truth for the cheadergen invocation. Called by:
#   - the top-level Makefile's `generate-rust-headers` target
#   - the `cheadergen_generate` CMake custom target in this directory's
#     CMakeLists.txt
#
# `env -u CARGO_BUILD_TARGET RUSTFLAGS=` scrubs parent-build env so flags
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

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

exec env -u CARGO_BUILD_TARGET RUSTFLAGS= cheadergen generate \
    --lang c \
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
    --package=rqe_iterator_type \
    --package=rqe_iterators \
    --package=search_result \
    --package=query_node_type \
    --config=cheadergen.toml \
    --output-dir=headers \
    c_entrypoint
