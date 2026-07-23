#!/usr/bin/env bash
#
# Apply include-what-you-use fixes to the given paths (default: src).
# Run from the repo root.
#
#   scripts/iwyu.sh src/util        # apply to a folder
#   scripts/iwyu.sh src/config.c    # apply to a file
#
set -euo pipefail

DB=bin/linux-x64-debug-cov/search-community   # build dir holding compile_commands.json

iwyu_tool -p "$DB" "${@:-src}" \
  | fix_include --nocomments --ignore_re 'query_parser/v2'
