#!/bin/bash
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1).

# Build and install liburing from source
# This is needed for platforms where the repo version is too old or unavailable
#
# Usage: [sudo] ./install-liburing.sh [--prefix=/usr]
#
# Note: This script requires root privileges for 'make install' and 'ldconfig'.
# Run with sudo or as root in a container.

set -euo pipefail

PREFIX="/usr"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix=*)
            PREFIX="${1#*=}"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--prefix=/usr]"
            exit 1
            ;;
    esac
done

echo "=============================================="
echo "  Installing liburing from source"
echo "=============================================="
echo "Prefix: $PREFIX"
echo "Kernel: $(uname -r)"
echo ""

# Step 1: Check kernel support for io_uring
echo "--- Step 1: Checking kernel io_uring support ---"
if [ -r /proc/kallsyms ]; then
    URING_SYMBOLS=$(grep -c io_uring /proc/kallsyms 2>/dev/null || echo "0")
    if [ "$URING_SYMBOLS" -gt 0 ]; then
        echo "✓ Kernel has io_uring support ($URING_SYMBOLS symbols found)"
    else
        echo "✗ Kernel does not support io_uring (no symbols found in /proc/kallsyms)"
        echo "  io_uring requires kernel 5.1+ with CONFIG_IO_URING=y"
        exit 1
    fi
else
    echo "⚠ Cannot verify kernel io_uring support (kallsyms not readable)"
    echo "  This is normal in some container environments, continuing..."
fi
echo ""

# Step 2: Build and install liburing
echo "--- Step 2: Building liburing ---"
WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT

# Pin to a specific version for reproducibility
# https://github.com/axboe/liburing (official repo by Jens Axboe, io_uring maintainer)
#
# Version 2.1 matches what Redis Flex uses (Ubuntu 22.04's liburing-dev package).
# liburing maintains a stable ABI, so minor version differences shouldn't cause issues.
# Production version is determined by Redis Enterprise; this is for CI testing only.
LIBURING_VERSION="liburing-2.1"

echo "Cloning liburing ($LIBURING_VERSION)..."
git clone --depth 1 --branch "$LIBURING_VERSION" https://github.com/axboe/liburing.git "$WORK_DIR/liburing"

cd "$WORK_DIR/liburing"

echo "Configuring..."
./configure --prefix="$PREFIX"

echo "Building (library only)..."
make -C src -j"$(nproc)"

echo "Installing..."
make install

echo "Updating dynamic linker cache..."
ldconfig
echo ""

# Step 3: Verify installation
echo "--- Step 3: Verifying installation ---"
INSTALL_OK=true

if [ -f "$PREFIX/lib/liburing.so" ] || [ -f "$PREFIX/lib/liburing.so.2" ]; then
    echo "✓ Shared library: installed"
    ls -la "$PREFIX/lib/liburing.so"* 2>/dev/null | head -3 || true
else
    echo "✗ Shared library: NOT FOUND"
    INSTALL_OK=false
fi

if [ -f "$PREFIX/lib/liburing.a" ]; then
    echo "✓ Static library: installed"
else
    echo "✗ Static library: NOT FOUND"
    INSTALL_OK=false
fi

if [ -f "$PREFIX/include/liburing.h" ]; then
    echo "✓ Header file: installed"
else
    echo "✗ Header file: NOT FOUND"
    INSTALL_OK=false
fi
echo ""

# Step 4: Test io_uring runtime support (kernel + Docker/seccomp)
echo "--- Step 4: Testing io_uring runtime support ---"
cat > "$WORK_DIR/test_uring.c" << 'EOF'
#include <liburing.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>

int main() {
    struct io_uring ring;
    int ret = io_uring_queue_init(1, &ring, 0);
    if (ret < 0) {
        if (ret == -EPERM) {
            printf("BLOCKED: Permission denied (seccomp/Docker restriction)\n");
        } else if (ret == -ENOSYS) {
            printf("UNAVAILABLE: Kernel does not support io_uring\n");
        } else {
            printf("FAILED: %s (error %d)\n", strerror(-ret), -ret);
        }
        return 1;
    }
    printf("WORKING: io_uring initialized successfully\n");
    io_uring_queue_exit(&ring);
    return 0;
}
EOF

if gcc -o "$WORK_DIR/test_uring" "$WORK_DIR/test_uring.c" -L"$PREFIX/lib" -I"$PREFIX/include" -luring -Wl,-rpath,"$PREFIX/lib" 2>/dev/null; then
    URING_STATUS=$("$WORK_DIR/test_uring" 2>&1) || true
    case "$URING_STATUS" in
        WORKING*)
            echo "✓ io_uring runtime: $URING_STATUS"
            ;;
        BLOCKED*)
            echo "✗ io_uring runtime: $URING_STATUS"
            echo "  Hint: Run Docker with --security-opt seccomp=unconfined"
            INSTALL_OK=false
            ;;
        *)
            echo "✗ io_uring runtime: $URING_STATUS"
            INSTALL_OK=false
            ;;
    esac
else
    echo "⚠ Could not compile io_uring test (will verify at build time)"
fi
echo ""

# Summary
echo "=============================================="
if [ "$INSTALL_OK" = true ]; then
    echo "  liburing installation: SUCCESS"
else
    echo "  liburing installation: COMPLETED WITH WARNINGS"
    echo "  Some features may not work correctly"
fi
echo "=============================================="
