# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

cmake_minimum_required(VERSION 3.13)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# VectorSimilarity headers (from deps, built as part of RediSearch)
set(VECSIM_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../deps/RediSearch/deps/VectorSimilarity)

# Source files for the main library (includes SpeeDBStore which requires SpeedB)
file(GLOB FACTORY_SOURCES src/factory/*.cpp)
set(VECSIM_DISK_SOURCES
    ${FACTORY_SOURCES}
    src/speedb_store.cpp
    src/vecsim_disk_api.cpp
    src/storage/edge_merge_operator.cpp
)

# Create the library
add_library(vecsim_disk STATIC ${VECSIM_DISK_SOURCES})

# Include directories
target_include_directories(vecsim_disk PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${VECSIM_ROOT}/src  # VectorSimilarity headers - PUBLIC since vecsim_disk extends VecSim
    ${CMAKE_CURRENT_SOURCE_DIR}/../deps/speedb-ent/include  # SpeedB/RocksDB C API headers
)

# Link against VectorSimilarity for base class and types
target_link_libraries(vecsim_disk PUBLIC VectorSimilarity)

# Link against SpeedB (built by CMake speedb-shared target in root CMakeLists.txt)
target_link_libraries(vecsim_disk PUBLIC ${SPEEDB_LIB})

# Add dependency on speedb-shared target to ensure it's built first
add_dependencies(vecsim_disk speedb-shared)

# SpeedB requires C++ standard library
if(APPLE)
    target_link_libraries(vecsim_disk PUBLIC c++)
else()
    target_link_libraries(vecsim_disk PUBLIC stdc++)
endif()

# =============================================================================
# Unit Tests (optional, enabled via BUILD_TESTS)
# =============================================================================
option(BUILD_TESTS "Build vecsim_disk unit tests" ON)

if(BUILD_TESTS)
    # Add test helpers include path so hnsw_disk.h can find hnsw_disk_test_helpers.h
    target_include_directories(vecsim_disk PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit
    )

    # Enable testing
    enable_testing()

    # Add tests subdirectory
    add_subdirectory(tests)
endif()

# =============================================================================
# Benchmarks (optional, enabled via BUILD_VECSIM_BENCHMARKS)
# =============================================================================
option(BUILD_VECSIM_BENCHMARKS "Build vecsim_disk benchmarks" OFF)

if(BUILD_VECSIM_BENCHMARKS)
    # Fetch Google Benchmark if not already available
    include(FetchContent)
    option(BENCHMARK_ENABLE_GTEST_TESTS "" OFF)
    option(BENCHMARK_ENABLE_TESTING "" OFF)
    FetchContent_Declare(
        google_benchmark
        URL https://github.com/google/benchmark/archive/refs/tags/v1.9.5.zip
    )
    FetchContent_MakeAvailable(google_benchmark)

    add_subdirectory(benchmarks)
endif()
