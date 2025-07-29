###############################################################################
# CMake configuration for the disk-based storage module
#
# This module provides persistent storage for RediSearch using RocksDB/Speedb
# as the underlying storage engine.
###############################################################################

# Set C++ standard to C++20 for modern features
set(CMAKE_CXX_STANDARD 20)

# Collect all C++ source files in this directory and subdirectories
file(GLOB_RECURSE SOURCES "*.cpp")

# Configure the Speedb external project
# Speedb is a high-performance fork of RocksDB used for persistent storage
include(ExternalProject)
ExternalProject_Add(speedb
    # Source code location
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/speedb-ent

    # Build directory
    BINARY_DIR ${CMAKE_BINARY_DIR}/speedb-build

    # Pass through the build type and PIC settings to ensure consistent optimization and compatibility
    # Build as shared library for dynamic linking
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DBUILD_SHARED_LIBS=ON
               -DWITH_TESTS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_TOOLS=OFF

    # Build the speedb target with parallel jobs
    BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/speedb-build --target speedb -j6

    # Skip installation step
    INSTALL_COMMAND ""
)

# Configure the CRoaring external project
# CRoaring is a library for Roaring Bitmaps - build as static library
ExternalProject_Add(croaring
    # Source code location
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/CRoaring

    # Build directory
    BINARY_DIR ${CMAKE_BINARY_DIR}/croaring-build

    # Pass through the build type and PIC settings to ensure consistent optimization and compatibility
    # Build as static library for static linking
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DROARING_BUILD_STATIC=ON
               -DBUILD_SHARED_LIBS=OFF
               -DENABLE_ROARING_TESTS=OFF

    # Build the roaring target with parallel jobs
    BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/croaring-build --target roaring -j6

    # Skip installation step
    INSTALL_COMMAND ""
)

# Create the redisearch-disk library from our source files
# Using STATIC here but with PIC enabled for compatibility with shared libraries
add_library(redisearch-disk STATIC ${SOURCES})

# Set Position Independent Code (PIC) for this specific target
set_target_properties(redisearch-disk PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Include Speedb and CRoaring headers
target_include_directories(redisearch-disk PRIVATE
    ${CMAKE_SOURCE_DIR}/deps/speedb-ent/include
    ${CMAKE_SOURCE_DIR}/deps/CRoaring/include
    ${CMAKE_SOURCE_DIR}/deps/CRoaring/cpp
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/src  # Include RediSearch headers for integration
)

# Add dependencies to ensure Speedb and CRoaring are built before redisearch-disk
add_dependencies(redisearch-disk speedb croaring)

# Link against CRoaring statically (Speedb will be linked dynamically by the main target)
target_link_libraries(redisearch-disk PUBLIC
    ${CMAKE_BINARY_DIR}/croaring-build/src/libroaring.a
)

# Enable ccache for faster builds if available
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    # Configure compiler launchers to use ccache
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
else()
    message(WARNING "ccache not found. Install it for faster builds.")
endif()
