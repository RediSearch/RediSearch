#!/usr/bin/env bash
#
# Verify that every cheadergen-generated header under headers/ is
# self-contained: that including it on its own (with the project include
# paths available) compiles, without relying on some other header having
# been included first.
#
# Why this exists
# ---------------
# cheadergen keys the C `#include` directives it injects by *output header*
# (the `[header.X] includes = [...]` entries in cheadergen.toml), not by the
# type that needs them. So when an FFI function moves between crates, the
# include directive does not follow it automatically. The breakage is
# silent: the header still compiles everywhere it is used *after* some other
# header (e.g. redismodule.h) has already been included, and only fails when
# included standalone. This check forces the standalone case so a missing
# include fails loudly, at the header that introduced it.
#
# Include paths are not hardcoded here: they are passed in as `-I<dir>` flags
# by the caller, so there is a single source of truth. The
# `cheadergen_check_self_contained` CMake target supplies them from the
# project's own `include_directories(...)` (the INCLUDE_DIRECTORIES directory
# property), so this list cannot drift from the real C build.
#
# Usage:
#   check_headers_self_contained.sh -I<dir> [-I<dir> ...]

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
HEADERS_DIR="$SCRIPT_DIR/headers"

CC="${CC:-cc}"

inc_flags=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -I)
            inc_flags+=("-I$2")
            shift 2
            ;;
        -I*)
            inc_flags+=("$1")
            shift
            ;;
        *)
            echo "error: unexpected argument '$1' (only -I<dir> flags are accepted)" >&2
            exit 2
            ;;
    esac
done

if [[ "${#inc_flags[@]}" -eq 0 ]]; then
    echo "error: no include directories given." >&2
    echo "       Run via the 'cheadergen_check_self_contained' CMake target, or" >&2
    echo "       pass -I<dir> flags mirroring the project's include_directories()." >&2
    exit 2
fi

# Pre-existing headers that are not yet self-contained: they reference C
# types (Reducer, FieldSpec, IndexSpec, ...) without including the headers
# that define them, relying on include order via redisearch.h in the real
# build. Baselined so this check stays green and guards against *new*
# regressions; these should be fixed and removed from the list over time.
KNOWN_NOT_SELF_CONTAINED=(
    numeric_range_tree_ffi.h
    reducers_ffi.h
    rlookup_ffi.h
)

is_baselined() {
    local needle="$1"
    for h in "${KNOWN_NOT_SELF_CONTAINED[@]}"; do
        [[ "$h" == "$needle" ]] && return 0
    done
    return 1
}

err_file="$(mktemp "${TMPDIR:-/tmp}/hdrcheck.XXXXXX")"
trap 'rm -f "$err_file"' EXIT

failures=0
checked=0
unexpected_pass=0
for header in "$HEADERS_DIR"/*.h; do
    name="$(basename "$header")"
    checked=$((checked + 1))
    # One translation unit per header: include only this header so a missing
    # include cannot be masked by a sibling already pulled in.
    if printf '#include "%s"\n' "$name" \
        | "$CC" -fsyntax-only -x c -std=gnu11 "${inc_flags[@]}" - 2>"$err_file"; then
        if is_baselined "$name"; then
            echo "NOW SELF-CONTAINED (remove from baseline): $name"
            unexpected_pass=$((unexpected_pass + 1))
        fi
    elif is_baselined "$name"; then
        echo "known not self-contained (baselined): $name"
    else
        echo "NOT SELF-CONTAINED: $name"
        sed 's/^/    /' "$err_file"
        failures=$((failures + 1))
    fi
done

if [[ "$unexpected_pass" -gt 0 ]]; then
    echo "FAIL: $unexpected_pass baselined header(s) now compile standalone; remove them from KNOWN_NOT_SELF_CONTAINED."
    exit 1
fi

if [[ "$failures" -gt 0 ]]; then
    echo "FAIL: $failures of $checked generated header(s) are not self-contained."
    exit 1
fi
echo "OK: all $checked generated headers are self-contained."
