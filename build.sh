#!/bin/bash

# Simple build script for RediSearch with disk storage
# Usage: ./build.sh [clean]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== RediSearch Disk Build Script ===${NC}"

# Check if clean is requested
if [ "$1" = "clean" ]; then
    echo -e "${YELLOW}Cleaning build directory...${NC}"
    rm -rf build
    echo -e "${GREEN}Build directory cleaned.${NC}"
    exit 0
fi

# Create build directory
echo -e "${YELLOW}Creating build directory...${NC}"
mkdir -p build
cd build

# Run CMake configuration
echo -e "${YELLOW}Configuring with CMake...${NC}"
cmake .. -DUSE_DISK_STORAGE=ON -DCMAKE_BUILD_TYPE=Release

# Build the project
echo -e "${YELLOW}Building project...${NC}"
make -j$(nproc)

# Check if build was successful
if [ $? -eq 0 ]; then
    echo -e "${GREEN}=== Build completed successfully! ===${NC}"
    echo -e "${GREEN}Output: $(pwd)/redisearch.so${NC}"
    
    # Show file info
    if [ -f "redisearch.so" ]; then
        echo -e "${YELLOW}File size: $(du -h redisearch.so | cut -f1)${NC}"
        echo -e "${YELLOW}File type: $(file redisearch.so)${NC}"
    else
        echo -e "${RED}Warning: redisearch.so not found in expected location${NC}"
        echo -e "${YELLOW}Looking for .so files:${NC}"
        find . -name "*.so" -type f
    fi
else
    echo -e "${RED}=== Build failed! ===${NC}"
    exit 1
fi
