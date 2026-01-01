cmake_minimum_required(VERSION 3.13)
project(redisearch-disk)

# Set C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_STANDARD 17)

# Always use .so extension even on macOS
set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
# 1. Build RediSearch as static library
#----------------------------------------------------------------------------------------------

# Set variables needed by RediSearch
set(MODULE_NAME "search")
set(COORD_TYPE "oss")

# Configure RediSearch to build as static library when included as subdirectory. We need this
# for redisearch to register its commands correctly.
set(REDISEARCH_BUILD_SHARED OFF)

# Include RediSearch - this creates a static library named 'redisearch'
add_subdirectory(deps/RediSearch)

#----------------------------------------------------------------------------------------------
# 2. Build disk code as static library
#----------------------------------------------------------------------------------------------

add_subdirectory(redisearch_disk)

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

# Link the static libraries with selective whole-archive
# macOS uses -force_load, Linux uses --whole-archive
if(APPLE)
    target_link_libraries(redisearch-module
        -Wl,-force_load,$<TARGET_FILE:redisearch>
        -Wl,-force_load,$<TARGET_FILE:redisearch-disk>
        -Wl,-force_load,$<TARGET_FILE:vecsim_disk>
    )
else()
    target_link_libraries(redisearch-module
        -Wl,--whole-archive
        redisearch          # RediSearch static library (contains RedisModule_OnLoad and all its dependencies)
        redisearch-disk     # Disk code static library
        vecsim_disk         # VecSim disk code static library
        -Wl,--no-whole-archive
    )
endif()

# Ensure dependencies are built first
add_dependencies(redisearch-module redisearch redisearch-disk vecsim_disk)
