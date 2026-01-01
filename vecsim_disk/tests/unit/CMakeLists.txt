# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

cmake_minimum_required(VERSION 3.13)

# Find GTest
find_package(GTest REQUIRED)

# Unit test sources
set(UNIT_TEST_SOURCES
    test_hnsw_disk.cpp
)

# Create test executable
add_executable(vecsim_disk_unit_tests ${UNIT_TEST_SOURCES})

target_link_libraries(vecsim_disk_unit_tests
    vecsim_disk
    GTest::gtest
    GTest::gtest_main
)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(vecsim_disk_unit_tests)

