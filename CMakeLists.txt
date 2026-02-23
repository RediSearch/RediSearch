cmake_minimum_required(VERSION 3.13)
project(redisearch-disk)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Print compiler information for CI debugging
message(STATUS "=== Compiler Information ===")
message(STATUS "C compiler:   ${CMAKE_C_COMPILER}")
message(STATUS "C compiler ID: ${CMAKE_C_COMPILER_ID}")
message(STATUS "C compiler version: ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "CXX compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "CXX compiler ID: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "CXX compiler version: ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "==============================")

# Detect and configure compiler cache (sccache or ccache)
# This speeds up rebuilds by caching compilation results
find_program(SCCACHE_PROGRAM sccache)
find_program(CCACHE_PROGRAM ccache)

if(SCCACHE_PROGRAM)
    message(STATUS "Found sccache: ${SCCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER "${SCCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SCCACHE_PROGRAM}")
elseif(CCACHE_PROGRAM)
    message(STATUS "Found ccache: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER "${CCACHE_PROGRAM}")
else()
    message(STATUS "No compiler cache found (sccache or ccache). Consider installing one for faster rebuilds.")
endif()

# Basic compiler flags - conditional optimization based on debug mode
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "-fPIC -g -pthread -O0")
    set(CMAKE_CXX_FLAGS "-fPIC -g -pthread -O0")
    set(RUST_PROFILE "dev")
    message(STATUS "Debug build: Using -O0 optimization")
else()
    set(CMAKE_C_FLAGS "-fPIC -g -pthread -O3")
    set(CMAKE_CXX_FLAGS "-fPIC -g -pthread -O3")
    set(RUST_PROFILE "release")
    message(STATUS "Release build: Using -O3 optimization")
endif()

# Platform-specific linker flags for shared library
if(APPLE)
    set(SHARED_LINKER_FLAGS "-undefined dynamic_lookup")
else()
    set(SHARED_LINKER_FLAGS "-shared -Bsymbolic")
endif()

#----------------------------------------------------------------------------------------------
# 0. Build Speedb shared library
#----------------------------------------------------------------------------------------------

# Speedb source directory
set(SPEEDB_SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/speedb-ent")
set(SPEEDB_BUILD_DIR "${CMAKE_BINARY_DIR}/speedb-build")
# Create SPEEDB_BUILD_DIR if it doesn't exist
file(MAKE_DIRECTORY "${SPEEDB_BUILD_DIR}")

# Determine build type for Speedb (match main project)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(SPEEDB_BUILD_TYPE "Debug")
else()
    set(SPEEDB_BUILD_TYPE "RelWithDebInfo")
endif()

# Get number of processors for parallel build
include(ProcessorCount)
ProcessorCount(NPROC)
if(NPROC EQUAL 0)
    set(NPROC 1)
endif()

# Build Speedb shared library as a custom target
# This runs cmake configure and build during the build phase
set(SPEEDB_LIB "${SPEEDB_BUILD_DIR}/libspeedb.so")
add_custom_target(speedb-shared
    COMMAND ${CMAKE_COMMAND} -E make_directory "${SPEEDB_BUILD_DIR}"
    COMMAND ${CMAKE_COMMAND}
        -DCMAKE_BUILD_TYPE=${SPEEDB_BUILD_TYPE}
        -DWITH_GFLAGS=0
        -DUSE_ASYNC=1
        -DPORTABLE=1
        ${SPEEDB_SOURCE_DIR}
        WORKING_DIRECTORY ${SPEEDB_BUILD_DIR}
    COMMAND ${CMAKE_COMMAND} --build . --target speedb-shared -j${NPROC}
        WORKING_DIRECTORY ${SPEEDB_BUILD_DIR}
    COMMENT "Building Speedb shared library"
    BYPRODUCTS "${SPEEDB_LIB}"
)

set(SPEEDB_LIB_DIR "${SPEEDB_BUILD_DIR}")

message(STATUS "Speedb library will be built at: ${SPEEDB_LIB}")

#----------------------------------------------------------------------------------------------
# 1. Build RediSearch as static library
#----------------------------------------------------------------------------------------------

# Set variables needed by RediSearch
set(MODULE_NAME "search")
set(COORD_TYPE "oss")

# Configure RediSearch to build as static library when included as subdirectory. We need this
# for redisearch to register its commands correctly.
set(REDISEARCH_BUILD_SHARED OFF)

# Master build tests option - controls whether vecsim_disk tests are built.
# This does NOT define BUILD_TESTS macro globally - that would affect VectorSimilarity
# and require building the serializer library (which we want to avoid).
# Default ON for development; set to OFF for production/library builds.
option(BUILD_TESTS "Build vecsim_disk tests" ON)

# Don't build VectorSimilarity tests/benchmarks - we don't run them, and they add
# significant build time (googletest, google_benchmark fetch + compile).
# Also don't define BUILD_TESTS macro for VectorSimilarity - that would make HNSWIndex
# inherit from HNSWSerializer, requiring the serializer library to be built.
set(VECSIM_BUILD_TESTS OFF CACHE BOOL "Build vecsim tests" FORCE)

# Include RediSearch - this creates a static library named 'redisearch'
add_subdirectory(deps/RediSearch)

#----------------------------------------------------------------------------------------------
# 2. Build disk code as static library
#----------------------------------------------------------------------------------------------

add_subdirectory(redisearch_disk)
add_dependencies(redisearch-disk_build speedb-shared)

#----------------------------------------------------------------------------------------------
# 3. Build vecsim_disk as static library
#----------------------------------------------------------------------------------------------

add_subdirectory(vecsim_disk)

#----------------------------------------------------------------------------------------------
# 4. Create final shared library linking all static libraries
#----------------------------------------------------------------------------------------------

# Create the final Redis module using an empty file
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/empty.c "")
add_library(redisearch-module SHARED empty.c)

# Set output name to redisearch.so
set_target_properties(redisearch-module PROPERTIES
    OUTPUT_NAME "redisearch"
    PREFIX ""
    LINKER_LANGUAGE CXX
    LINK_FLAGS "${SHARED_LINKER_FLAGS}"
)

# SPEEDB_LIB and SPEEDB_LIB_DIR are set above when building speedb-shared

# Link the static libraries with selective whole-archive
# macOS uses -force_load, Linux uses --whole-archive
if(APPLE)
    target_link_libraries(redisearch-module
        -Wl,-force_load,$<TARGET_FILE:redisearch>
        -Wl,-force_load,$<TARGET_FILE:redisearch-disk>
        -Wl,-force_load,$<TARGET_FILE:vecsim_disk>
        ${SPEEDB_LIB}
    )
    # Set rpath for macOS
    set_target_properties(redisearch-module PROPERTIES
        INSTALL_RPATH "${SPEEDB_LIB_DIR}"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
else()
    target_link_libraries(redisearch-module
        -Wl,--whole-archive
        redisearch          # RediSearch static library (contains RedisModule_OnLoad and all its dependencies)
        redisearch-disk     # Disk code static library
        vecsim_disk         # VecSim disk code static library
        -Wl,--no-whole-archive
        ${SPEEDB_LIB}       # Link libspeedb.so dynamically
    )
    # Set rpath for Linux so the library can be found at runtime
    set_target_properties(redisearch-module PROPERTIES
        INSTALL_RPATH "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX:/opt/redislabs/lib:${SPEEDB_LIB_DIR}"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

# Ensure dependencies are built first (including speedb-shared)
add_dependencies(redisearch-module redisearch redisearch-disk vecsim_disk speedb-shared)
