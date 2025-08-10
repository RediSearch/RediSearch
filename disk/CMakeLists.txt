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

# Build external dependencies needed by disk code
include(ExternalProject)

# Build Speedb (RocksDB fork)
ExternalProject_Add(speedb
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/speedb-ent
    BINARY_DIR ${CMAKE_BINARY_DIR}/speedb-build
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DBUILD_SHARED_LIBS=OFF
               -DWITH_TESTS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_TOOLS=OFF
               -DCMAKE_CXX_FLAGS=-frtti
               -DUSE_RTTI=ON
    BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/speedb-build --target speedb -j6
    INSTALL_COMMAND ""
)

# Build CRoaring
ExternalProject_Add(croaring
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/CRoaring
    BINARY_DIR ${CMAKE_BINARY_DIR}/croaring-build
    CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DCMAKE_POSITION_INDEPENDENT_CODE=ON
               -DROARING_BUILD_STATIC=ON
               -DBUILD_SHARED_LIBS=OFF
               -DENABLE_ROARING_TESTS=OFF
    BUILD_COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR}/croaring-build --target roaring -j6
        INSTALL_COMMAND ""
)

# Create the redisearch-disk library from our source files
# Using STATIC here but with PIC enabled for compatibility with shared libraries
add_library(redisearch-disk STATIC ${SOURCES})

# Set Position Independent Code (PIC) for this specific target
set_target_properties(redisearch-disk PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Include headers needed by disk code
target_include_directories(redisearch-disk PRIVATE
    ${CMAKE_SOURCE_DIR}                      # For disk/ prefix includes
    ${CMAKE_SOURCE_DIR}/deps/speedb-ent/include
    ${CMAKE_SOURCE_DIR}/deps/CRoaring/include
    ${CMAKE_SOURCE_DIR}/deps/CRoaring        # For cpp/ prefix includes (cpp/roaring/roaring64map.hh)
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/src
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/src/redisearch_rs/headers  # For types_rs.h
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/src/buffer  # For buffer.h
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/deps  # For rmutil/ prefix includes
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/deps/VectorSimilarity/src  # For VecSim/ prefix includes
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/deps/RedisModulesSDK
    ${CMAKE_SOURCE_DIR}/deps/RediSearch/.install/boost  # Boost from RediSearch
)

# Add dependencies
add_dependencies(redisearch-disk speedb croaring)

# Link external libraries
target_link_libraries(redisearch-disk PRIVATE
    ${CMAKE_BINARY_DIR}/speedb-build/libspeedb.a
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
