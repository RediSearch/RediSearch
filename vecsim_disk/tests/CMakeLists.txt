# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

# =============================================================================
# Unit Tests for vecsim_disk
# =============================================================================

# Fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.16.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Enable testing and include GoogleTest module for test discovery
enable_testing()
include(GoogleTest)

function(add_vecsim_test test_name source_file)
    # Create test executable
    add_executable(${test_name} ${source_file})

    # Add unit test directory to include path
    target_include_directories(${test_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)

    # Link against full vecsim_disk library (includes SpeedB transitively)
    target_link_libraries(${test_name} PRIVATE gtest gtest_main vecsim_disk)

    # Register with CTest
    gtest_discover_tests(${test_name})
endfunction()

# =============================================================================
# Auto-discover and add all test files
# =============================================================================

# Find all test_*.cpp files in the unit directory
file(GLOB TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_*.cpp)

# Add each test file
foreach(TEST_SOURCE ${TEST_SOURCES})
    # Extract filename without path and extension (e.g., test_encoding.cpp -> test_encoding)
    get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)

    # Add the test
    add_vecsim_test(${TEST_NAME} ${TEST_SOURCE})
endforeach()

# =============================================================================
# Special: Add big-endian variants of tests to verify cross-platform serialization
# =============================================================================

# Create a big-endian variant of test_encoding to test serialization on simulated big-endian
add_executable(test_encoding_bigendian ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_encoding.cpp)
target_include_directories(test_encoding_bigendian PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
target_link_libraries(test_encoding_bigendian PRIVATE gtest gtest_main vecsim_disk)
target_compile_definitions(test_encoding_bigendian PRIVATE FORCE_BIG_ENDIAN_TEST=1)
gtest_discover_tests(test_encoding_bigendian)

# Create a big-endian variant of test_hnsw_storage to test vector storage on simulated big-endian
add_executable(test_hnsw_storage_bigendian ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_hnsw_storage.cpp)
target_include_directories(test_hnsw_storage_bigendian PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
target_link_libraries(test_hnsw_storage_bigendian PRIVATE gtest gtest_main vecsim_disk)
target_compile_definitions(test_hnsw_storage_bigendian PRIVATE FORCE_BIG_ENDIAN_TEST=1)
gtest_discover_tests(test_hnsw_storage_bigendian)
