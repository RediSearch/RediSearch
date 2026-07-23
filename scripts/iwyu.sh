#!/usr/bin/env bash
#
# Apply include-what-you-use fixes to the given paths (default: src).
# Run from the repo root.
#
#   scripts/iwyu.sh src/util        # apply to a folder
#   scripts/iwyu.sh src/config.c    # apply to a file
#
set -euo pipefail

# Newest compile_commands.json under bin/
DB=$(find bin -name compile_commands.json -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
if [[ -z "${DB:-}" ]]; then
  echo "no compile_commands.json under bin/ — build once (./build.sh DEBUG=1) first." >&2
  exit 1
fi
echo "# compile db: $DB"

iwyu_tool -p "$DB" "${@:-src}" \
  | fix_include --nocomments --ignore_re '\.h$|query_parser/'
