#!/usr/bin/env bash
# Apply the boost-1.88 SHA1 API fix to a worktree (no-op if the worktree
# pre-dates the affected file or already has the fix).
#
# Older tags (v2.10.16..v2.10.30, v8.0.x, v8.2.x) declare Sha1::hash as
# uint32_t[5] and call get_digest(uint32_t[]) which boost 1.88 removed.
# The fix (officially landed in v8.4.9) stores the digest as unsigned char[20]
# and rebuilds the 32-bit words at format time.
set -uo pipefail

wt="${1:?usage: apply-boost-patch.sh <worktree-path>}"
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

dst="$wt/src/util/hash"
if [ ! -d "$dst" ]; then
  echo "== boost-patch: no $dst, skipping (tag pre-dates hash.cpp)"
  exit 0
fi
if [ ! -f "$dst/hash.cpp" ] || [ ! -f "$dst/hash.h" ]; then
  echo "== boost-patch: missing files in $dst, skipping"
  exit 0
fi

# Skip if already patched (idempotent).
if grep -q 'unsigned char hash\[20\]' "$dst/hash.h" 2>/dev/null; then
  echo "== boost-patch: already applied"
  exit 0
fi

cp "$HERE/patches/hash.cpp" "$dst/hash.cpp"
cp "$HERE/patches/hash.h"   "$dst/hash.h"
echo "== boost-patch: applied to $dst"
