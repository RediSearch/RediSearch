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

# Enable BUILD_TESTS macro for test executables to access test-only methods in HNSWDiskIndex
# (e.g., fitMemory, getDataByLabel, etc.)
add_compile_definitions(BUILD_TESTS)

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

# Define big-endian test variants for tests that involve serialization/encoding
set(BIGENDIAN_TEST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_encoding.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_hnsw_storage.cpp
)

set(BIGENDIAN_TEST_TARGETS "")
foreach(SOURCE_FILE ${BIGENDIAN_TEST_SOURCES})
    get_filename_component(BASE_NAME ${SOURCE_FILE} NAME_WE)
    set(TEST_NAME "${BASE_NAME}_bigendian")

    add_executable(${TEST_NAME} ${SOURCE_FILE})
    target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/unit)
    target_link_libraries(${TEST_NAME} PRIVATE gtest gtest_main vecsim_disk)
    target_compile_definitions(${TEST_NAME} PRIVATE FORCE_BIG_ENDIAN_TEST=1)
    gtest_discover_tests(${TEST_NAME})

    list(APPEND BIGENDIAN_TEST_TARGETS ${TEST_NAME})
endforeach()

# =============================================================================
# Convenience target to build all vecsim_disk tests
# =============================================================================

# Collect all test target names (regular + bigendian)
set(ALL_TEST_TARGETS ${BIGENDIAN_TEST_TARGETS})
foreach(TEST_SOURCE ${TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
    list(APPEND ALL_TEST_TARGETS ${TEST_NAME})
endforeach()

# Create a target that builds all tests
add_custom_target(vecsim_disk_tests DEPENDS ${ALL_TEST_TARGETS})
