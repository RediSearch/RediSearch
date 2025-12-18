#!/usr/bin/env bash

# Safe symbol extraction script that handles Alpine ARM64 issues
TARGET="$1"

# Check if we should skip debug symbol extraction
SKIP_EXTRACTION=false

if [ -f "/etc/alpine-release" ]; then
    # Alpine (any architecture) - skip to avoid musl/objcopy issues
    echo "Skipping debug symbol extraction on Alpine ($(uname -m)) to avoid musl/objcopy issues"
    SKIP_EXTRACTION=true
elif ! command -v objcopy >/dev/null 2>&1; then
    # objcopy not available - skip to avoid command not found errors
    echo "objcopy not available - skipping debug symbol extraction"
    SKIP_EXTRACTION=true
fi

if [ "$SKIP_EXTRACTION" = true ]; then
    exit 0
else
    # Extract debug symbols if objcopy is available
    echo "Extracting debug symbols for $TARGET"
    objcopy --only-keep-debug "$TARGET" "$TARGET.debug"
    objcopy --strip-debug "$TARGET"
    objcopy --add-gnu-debuglink "$TARGET.debug" "$TARGET"
    exit 0
fi
