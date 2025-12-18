#!/bin/bash

set -e

echo "=== RedisJSON Alpine ARM64 Test ==="

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Check if we're on ARM64 Mac (for proper emulation)
if [[ "$(uname -m)" == "arm64" ]]; then
    echo "✓ Running on ARM64 Mac - native ARM64 Docker support"
    PLATFORM_ARG="--platform linux/arm64"
else
    echo "⚠ Running on x86_64 - will use emulation (slower)"
    PLATFORM_ARG="--platform linux/arm64"
fi

# Build the test image
echo "Building Alpine ARM64 test image..."
docker build $PLATFORM_ARG \
    -f "$PROJECT_ROOT/build/docker/alpine-arm64-test.dockerfile" \
    -t redisjson-alpine-arm64-test \
    "$PROJECT_ROOT/build/docker/"

echo ""
echo "=== Running Alpine ARM64 Test ==="

# Run the container with the project mounted
docker run $PLATFORM_ARG --rm \
    -v "$PROJECT_ROOT:/workspace" \
    -w /workspace \
    redisjson-alpine-arm64-test \
    bash -c "
        echo '=== System Info ==='
        uname -a
        cat /etc/alpine-release
        echo ''
        
        echo '=== Building RedisJSON module ==='
        make build
        
        echo '=== Checking build output ==='
        ls -la bin/linux-arm64v8-release/ || echo 'No ARM64 build directory found'
        
        echo '=== Testing pack command ==='
        make pack
        
        echo '=== Checking artifacts ==='
        ls -la bin/artifacts/ || echo 'No artifacts directory found'
        
        echo '=== Test Complete ==='
        echo 'Alpine ARM64 test finished successfully!'
    "

echo ""
echo "=== Alpine ARM64 Test Completed ==="
