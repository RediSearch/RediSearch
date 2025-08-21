cmake_minimum_required(VERSION 3.13)
project(redisearch-disk)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Basic compiler flags - conditional optimization based on debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "-fPIC -g -pthread -O0")
    set(CMAKE_CXX_FLAGS "-fPIC -g -pthread -O0")
    message(STATUS "Debug build: Using -O0 optimization")
else()
    set(CMAKE_C_FLAGS "-fPIC -g -pthread -O3")
    set(CMAKE_CXX_FLAGS "-fPIC -g -pthread -O3")
    message(STATUS "Release build: Using -O3 optimization")
endif()

# Option to build C++ tests in this repository
option(BUILD_DISK_TESTS "Build C++ tests under tests/cpptests" OFF)

# Platform-specific linker flags for shared library
if(APPLE)
    set(SHARED_LINKER_FLAGS "-undefined dynamic_lookup")
else()
    set(SHARED_LINKER_FLAGS "-shared -Bsymbolic")
endif()

#----------------------------------------------------------------------------------------------
# 1. Build RediSearch as static library
#----------------------------------------------------------------------------------------------

# Set variables needed by RediSearch
set(MODULE_NAME "search")
set(COORD_TYPE "rlec")

# Configure RediSearch to build as static library when included as subdirectory
set(REDISEARCH_BUILD_SHARED OFF)

# Include RediSearch - this creates a static library named 'redisearch'
add_subdirectory(deps/RediSearch)

#----------------------------------------------------------------------------------------------
# 2. Build disk code as static library
#----------------------------------------------------------------------------------------------

add_subdirectory(disk)

#----------------------------------------------------------------------------------------------
# 3. Create final shared library linking both static libraries
#----------------------------------------------------------------------------------------------

# Create the final Redis module
add_library(redisearch-module SHARED redisearch_module.c)

#----------------------------------------------------------------------------------------------
# 4. Tests
#----------------------------------------------------------------------------------------------
if(BUILD_DISK_TESTS)
    enable_testing()
    add_subdirectory(tests/cpptests)
endif()

# Set output name to redisearch.so
set_target_properties(redisearch-module PROPERTIES
    OUTPUT_NAME "redisearch"
    PREFIX ""
    LINKER_LANGUAGE CXX
    LINK_FLAGS "${SHARED_LINKER_FLAGS}"
)

# Find liburing if available
find_package(uring)

# Ensure external dependencies are built before linking
add_dependencies(redisearch-module speedb croaring)

# Link the static libraries with selective whole-archive
# Also link liburing (via imported target if found, else fall back to -luring)
target_link_libraries(redisearch-module
    -Wl,--whole-archive
    redisearch          # RediSearch static library (contains RedisModule_OnLoad and all its dependencies)
    redisearch-disk     # Disk code static library
    -Wl,--no-whole-archive
    ${CMAKE_BINARY_DIR}/speedb-build/libspeedb.a      # Speedb static library (without whole-archive)
    ${CMAKE_BINARY_DIR}/croaring-build/src/libroaring.a  # CRoaring static library
    stdc++              # C++ standard library for Speedb
    pthread             # Threading library
    dl                  # Dynamic loading library
    $<IF:$<BOOL:${uring_FOUND}>,uring::uring,uring>
)

# Ensure dependencies are built first
add_dependencies(redisearch-module redisearch redisearch-disk)
