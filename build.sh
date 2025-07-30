#!/bin/bash

# Simple build script for RediSearch with disk storage
# Usage: ./build.sh [clean] [-d|--debug]

set -e  # Exit on any error

echo "=== RediSearch Disk Build Script ==="

# Parse arguments
BUILD_TYPE="Release"
for arg in "$@"; do
    case $arg in
        clean)
            echo "Cleaning build directory..."
            rm -rf build
            echo "Build directory cleaned."
            exit 0
            ;;
        -d|--debug)
            BUILD_TYPE="Debug"
            echo "Debug mode enabled - using -O0 optimization"
            ;;
        *)
            # Unknown argument
            ;;
    esac
done

# Build Rust components first
echo "Building Rust components..."
if [ ! -f "deps/RediSearch/bin/linux-x64-release/search-community/redisearch_rs/libredisearch_rs.a" ]; then
    echo "Building RediSearch Rust library..."
    cd deps/RediSearch/src/redisearch_rs
    cargo build --release
    cd ../../../..
    echo "Rust components built successfully."
else
    echo "Rust components already built."
fi

# Create build directory
echo "Creating build directory..."
mkdir -p build
cd build

# Set up library path for Rust components
echo "Setting up library paths..."
mkdir -p deps/search-community/linux-x64-release/search-community/redisearch_rs
if [ ! -f "deps/search-community/linux-x64-release/search-community/redisearch_rs/libredisearch_rs.a" ]; then
    cp ../deps/RediSearch/bin/linux-x64-release/search-community/redisearch_rs/libredisearch_rs.a \
       deps/search-community/linux-x64-release/search-community/redisearch_rs/
    echo "Rust library copied to expected location."
fi

# Run CMake configuration
echo "Configuring with CMake (BUILD_TYPE=$BUILD_TYPE)..."
cmake .. -DCMAKE_BUILD_TYPE=$BUILD_TYPE

# Build the project
echo "Building project..."
make -j$(nproc)

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "=== Build completed successfully! ==="
    echo "Output: $(pwd)/redisearch.so"

    # Show file info
    if [ -f "redisearch.so" ]; then
        echo "File size: $(du -h redisearch.so | cut -f1)"
        echo "File type: $(file redisearch.so)"
    else
        echo "Warning: redisearch.so not found in expected location"
        echo "Looking for .so files:"
        find . -name "*.so" -type f
    fi
else
    echo "=== Build failed! ==="
    exit 1
fi
