#!/usr/bin/env bash
# Build redisearch.so for a single git tag inside the mod15505-builder image.
#
# Usage: build-tag.sh <tag>
#
# Env vars:
#   REPO_ROOT  : path to a RediSearch git checkout (defaults to repo containing this script)
#   WORK_DIR   : where worktrees and outputs live (default: $PWD/mod-15505-work)
set -uo pipefail

tag="${1:?usage: build-tag.sh <tag>}"

# Resolve REPO_ROOT: env override, else two levels up from this script
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT="${REPO_ROOT:-$(cd "$HERE/.." && pwd)}"
WORK_DIR="${WORK_DIR:-$PWD/mod-15505-work}"

WT="$WORK_DIR/worktrees/mod15505-${tag}"
OUT="$WORK_DIR/modules"
LOG_DIR="$WORK_DIR/logs"
LOG="$LOG_DIR/build-${tag}.log"
mkdir -p "$OUT" "$LOG_DIR" "$WORK_DIR/worktrees"

out_so="$OUT/redisearch-${tag}.so"
if [ -f "$out_so" ]; then
  echo "== $tag : already built at $out_so, skipping"
  exit 0
fi

# Create worktree if missing.
if [ ! -d "$WT" ]; then
  echo "== $tag : creating worktree $WT"
  git -C "$REPO_ROOT" worktree add -f --detach "$WT" "$tag" 2>&1 | tail -3
  # Apply boost-1.88 hash patch where needed (silently skips if files absent).
  bash "$HERE/apply-boost-patch.sh" "$WT" || true
fi

# Init submodules from host (fast: uses host's network/git).
echo "== $tag : updating submodules"
(cd "$WT" && git submodule update --init --recursive --quiet 2>&1 | tail -5)

# Convert worktree's .git FILE (pointing to host paths invisible inside docker)
# to a minimal .git directory so cmake-driven git operations don't get confused.
if [ -f "$WT/.git" ]; then
  echo "== $tag : neutralizing .git (worktree -> standalone)"
  rm "$WT/.git"
  mkdir "$WT/.git"
  echo "ref: refs/heads/main" > "$WT/.git/HEAD"
fi

echo "== $tag : building in ubuntu:22.04 container, log=$LOG"
date | tee "$LOG"

# Strategy: try ./build.sh first (8.x); fall back to `make build` (2.10.x).
# Always avoid the coord-oss artifact (we want the standalone module).
docker run --rm \
  -v "$WT:/src" \
  -v "$OUT:/out" \
  mod15505-builder:latest \
  bash -c "
    set -e
    cd /src
    # If tag pins a specific nightly toolchain, install it.
    if [ -f .rust-nightly ]; then
      nightly=\$(tr -d ' \\n\\r' < .rust-nightly)
      if [ -n \"\$nightly\" ]; then
        echo \"== installing rust toolchain: \$nightly\"
        rustup toolchain install \"\$nightly\" --profile default --component clippy rustfmt 2>&1 | tail -5
      fi
    fi
    if [ -x ./build.sh ]; then
      echo '== using ./build.sh'
      ./build.sh
      so=\$(find /src/bin -name 'redisearch.so' 2>/dev/null | grep -v coord-oss | head -1)
    else
      echo '== using make build (standalone search)'
      make fetch || true
      make build
      so=\$(find /src/bin -name 'redisearch.so' 2>/dev/null | grep -v coord-oss | head -1)
    fi
    echo \"== found: \$so\"
    if [ -n \"\$so\" ]; then
      cp \"\$so\" /out/redisearch-${tag}.so
      echo \"== copied to /out/redisearch-${tag}.so\"
    else
      echo '!! no .so produced'; exit 1
    fi
  " 2>&1 | tee -a "$LOG" | tail -30
rc=${PIPESTATUS[0]}
date | tee -a "$LOG"
if [ "$rc" != 0 ]; then
  echo "!! $tag : build failed (rc=$rc), see $LOG"
  exit "$rc"
fi
ls -la "$out_so"
