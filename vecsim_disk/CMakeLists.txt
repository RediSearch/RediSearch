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
set(VECSIM_DISK_SOURCES
    src/hnsw_disk_factory.cpp
    src/speedb_store.cpp
)

# Create the library
add_library(vecsim_disk STATIC ${VECSIM_DISK_SOURCES})

# Include directories
target_include_directories(vecsim_disk PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${VECSIM_ROOT}/src  # VectorSimilarity headers - PUBLIC since vecsim_disk extends VecSim
)

# Link against VectorSimilarity for base class and types
target_link_libraries(vecsim_disk PUBLIC VectorSimilarity)

# =============================================================================
# Unit Tests (optional, enabled via VECSIM_DISK_BUILD_TESTS)
# =============================================================================
option(VECSIM_DISK_BUILD_TESTS "Build vecsim_disk unit tests" ON)

if(VECSIM_DISK_BUILD_TESTS)
    # Enable testing
    enable_testing()

    # Fetch GoogleTest
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.16.0.zip
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    # Create a test-only library without SpeeDBStore (avoids SpeedB dependency)
    # Uses a stub that returns nullptr for CreateSpeeDBStore
    add_library(vecsim_disk_test_lib STATIC
        src/hnsw_disk_factory.cpp
        tests/unit/speedb_store_stub.cpp
    )

    target_include_directories(vecsim_disk_test_lib PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${VECSIM_ROOT}/src
    )

    target_link_libraries(vecsim_disk_test_lib PUBLIC VectorSimilarity)

    # Test executable - links against test lib (no SpeedB required)
    add_executable(test_hnsw_disk
        tests/unit/test_hnsw_disk.cpp
    )

    target_include_directories(test_hnsw_disk PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/unit
        ${VECSIM_ROOT}/src
    )

    target_link_libraries(test_hnsw_disk PRIVATE
        gtest
        gtest_main
        vecsim_disk_test_lib
    )

    # Register tests with CTest
    include(GoogleTest)
    gtest_discover_tests(test_hnsw_disk)
endif()
