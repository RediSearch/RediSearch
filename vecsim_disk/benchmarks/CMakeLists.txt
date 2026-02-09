# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

cmake_minimum_required(VERSION 3.13)

project(VecSimDisk_Benchmark)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Common include directories for all benchmarks
set(BM_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../tests/unit  # For test_utils.h
)

# Helper function to add a benchmark executable
function(add_vecsim_benchmark name source)
    add_executable(${name} ${source})
    target_link_libraries(${name}
        vecsim_disk
        benchmark::benchmark
    )
    target_include_directories(${name} PRIVATE ${BM_INCLUDE_DIRS})
    # Add to the list of all benchmarks
    set_property(GLOBAL APPEND PROPERTY VECSIM_DISK_BENCHMARKS ${name})
endfunction()

# =============================================================================
# Benchmark executables - automatically add all run_files/*.cpp
# =============================================================================

file(GLOB BENCHMARK_RUN_FILES "${CMAKE_CURRENT_SOURCE_DIR}/run_files/*.cpp")
foreach(run_file ${BENCHMARK_RUN_FILES})
    get_filename_component(bm_name ${run_file} NAME_WE)
    add_vecsim_benchmark(${bm_name} ${run_file})
endforeach()

# =============================================================================
# Aggregate target to build all benchmarks
# =============================================================================

get_property(ALL_BENCHMARKS GLOBAL PROPERTY VECSIM_DISK_BENCHMARKS)
add_custom_target(vecsim_disk_benchmarks DEPENDS ${ALL_BENCHMARKS})
