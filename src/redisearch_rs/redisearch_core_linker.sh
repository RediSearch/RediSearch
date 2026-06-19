#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#
# Thin linker wrapper for the conditional disk-plugin build (LP1/LP2).
#
# `rustc` is the linker driver for the `redisearch_core` cdylib (`redisearch.so`).
# For a cdylib it auto-generates an anonymous version script
#   { global: <#[no_mangle] Rust syms>; local: *; };
# whose `local: *` localizes every C-core / VecSim symbol. The dlopened plugin
# `redisearch_disk.so` resolves a fixed set of C-core symbols from `redisearch.so`
# via RTLD_GLOBAL, so those names MUST appear in `redisearch.so`'s `.dynsym`.
#
# Empirically (GNU ld 2.38, LLD 21) no linker flag promotes a symbol that the
# version script marked local, and a second `--version-script` cannot be combined
# with `rustc`'s anonymous one. The only robust fix is to inject the needed names
# into the `global:` section of `rustc`'s own version script before the link.
#
# This wrapper does exactly that, and ONLY when it is producing
# `libredisearch_core.so` (so the sibling `redisearch_disk_plugin` /
# `search_shared` links are untouched). For everything else it is a transparent
# pass-through to the real C compiler driver.
#
# Inputs (env):
#   REAL_CC                       real cc driver to exec (default: "cc")
#   REDISEARCH_PLUGIN_EXPORTS_FILE  newline-separated symbol names to inject
set -euo pipefail

real_cc="${REAL_CC:-cc}"
exports_file="${REDISEARCH_PLUGIN_EXPORTS_FILE:-}"

# Determine the link output and the rustc version-script path from argv.
out=""
version_script=""
prev=""
for a in "$@"; do
  case "$prev" in
    -o) out="$a" ;;
  esac
  case "$a" in
    -o) ;; # handled via prev
    -Wl,--version-script=*) version_script="${a#-Wl,--version-script=}" ;;
    --version-script=*) version_script="${a#--version-script=}" ;;
  esac
  prev="$a"
done

# Only patch the redisearch_core cdylib link, and only if we have both a version
# script and a non-empty symbol list. Otherwise pass through unchanged.
if [[ "${out}" == *libredisearch_core.so ]] \
   && [[ -n "${version_script}" && -f "${version_script}" ]] \
   && [[ -n "${exports_file}" && -s "${exports_file}" ]]; then
  patched="${version_script}.plugin_exports"
  # Insert each symbol immediately after the `global:` line so it joins the
  # exported set; the `local: *;` that follows no longer captures them.
  {
    awk '
      /global:/ && !done {
        print
        while ((getline line < EXPORTS) > 0) {
          if (line != "") printf "    %s;\n", line
        }
        close(EXPORTS)
        done = 1
        next
      }
      { print }
    ' EXPORTS="${exports_file}" "${version_script}"
  } > "${patched}"

  # Rebuild argv with the patched version script substituted in place.
  new_args=()
  for a in "$@"; do
    case "$a" in
      -Wl,--version-script=*) new_args+=("-Wl,--version-script=${patched}") ;;
      --version-script=*) new_args+=("--version-script=${patched}") ;;
      *) new_args+=("$a") ;;
    esac
  done
  exec "${real_cc}" "${new_args[@]}"
fi

exec "${real_cc}" "$@"
