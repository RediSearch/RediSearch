cmake_minimum_required(VERSION 3.10)

if(NOT DEFINED root)
	get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../.. ABSOLUTE)
endif()

message("# VectorSimilarity/tests/unit root: " ${root})
message("# VectorSimilarity/tests/unit binroot: " ${binroot})

project(VectorSimilarity_UnitTest)

include_directories(../utils)
include(CTest)

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CLANG_SAN_FLAGS} ${LLVM_CXX_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")

enable_testing()

option(FP64_TESTS "Build fp 64 tests" OFF)

if(FP64_TESTS)
	add_definitions(-DFP64_TESTS)
endif()

if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "(aarch64)|(arm64)|(ARM64)|(armv8)|(armv9)")
	include(${root}/cmake/aarch64InstructionFlags.cmake)
else()
	include(${root}/cmake/x86_64InstructionFlags.cmake)
endif()

add_executable(test_hnsw ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_hnsw.cpp test_hnsw_multi.cpp test_hnsw_tiered.cpp unit_test_utils.cpp)
add_executable(test_hnsw_sq8 ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_hnsw_sq8.cpp unit_test_utils.cpp)
add_executable(test_bruteforce ../utils/test_main_with_timeout.cpp test_bruteforce.cpp test_bruteforce_multi.cpp ../utils/mock_thread_pool.cpp unit_test_utils.cpp)
add_executable(test_allocator ../utils/test_main_with_timeout.cpp test_allocator.cpp ../utils/mock_thread_pool.cpp unit_test_utils.cpp)
add_executable(test_spaces ../utils/test_main_with_timeout.cpp test_spaces.cpp)
add_executable(test_types ../utils/test_main_with_timeout.cpp test_types.cpp)
add_executable(test_common ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_common.cpp unit_test_utils.cpp)
add_executable(test_components ../utils/test_main_with_timeout.cpp test_components.cpp ../utils/mock_thread_pool.cpp unit_test_utils.cpp)
add_executable(test_bf16 ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_bf16.cpp unit_test_utils.cpp)
add_executable(test_fp16 ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_fp16.cpp unit_test_utils.cpp)
add_executable(test_int8 ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_int8.cpp unit_test_utils.cpp)
add_executable(test_uint8 ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_uint8.cpp unit_test_utils.cpp)
add_executable(test_index_test_utils ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_index_test_utils.cpp unit_test_utils.cpp)
add_executable(test_svs ../utils/test_main_with_timeout.cpp ../utils/mock_thread_pool.cpp test_svs.cpp test_svs_tiered.cpp test_svs_multi.cpp test_svs_fp16.cpp test_svs_threadpool.cpp unit_test_utils.cpp)

target_link_libraries(test_hnsw PUBLIC gtest VectorSimilarity)
target_link_libraries(test_hnsw_sq8 PUBLIC gtest VectorSimilarity)
target_link_libraries(test_bruteforce PUBLIC gtest VectorSimilarity)
target_link_libraries(test_allocator PUBLIC gtest VectorSimilarity)
target_link_libraries(test_spaces PUBLIC gtest VectorSimilarity)
target_link_libraries(test_common PUBLIC gtest VectorSimilarity)
target_link_libraries(test_components PUBLIC gtest VectorSimilarity)
target_link_libraries(test_types PUBLIC gtest VectorSimilarity)
target_link_libraries(test_bf16 PUBLIC gtest VectorSimilarity)
target_link_libraries(test_fp16 PUBLIC gtest VectorSimilarity)
target_link_libraries(test_int8 PUBLIC gtest VectorSimilarity)
target_link_libraries(test_uint8 PUBLIC gtest VectorSimilarity)
target_link_libraries(test_index_test_utils PUBLIC gtest VectorSimilarity)
target_link_libraries(test_svs PUBLIC gtest VectorSimilarity)

include(GoogleTest)

gtest_discover_tests(test_hnsw)
gtest_discover_tests(test_hnsw_sq8)
gtest_discover_tests(test_bruteforce)
gtest_discover_tests(test_allocator)
gtest_discover_tests(test_spaces)
gtest_discover_tests(test_common)
gtest_discover_tests(test_components)
gtest_discover_tests(test_types)
gtest_discover_tests(test_bf16 TEST_PREFIX BF16UNIT_)
gtest_discover_tests(test_fp16 TEST_PREFIX FP16UNIT_)
gtest_discover_tests(test_int8 TEST_PREFIX INT8UNIT_)
gtest_discover_tests(test_uint8 TEST_PREFIX UINT8UNIT_)
gtest_discover_tests(test_index_test_utils)
# SVS Tiered tests are slow in debug mode, so we set a longer timeout
gtest_discover_tests(test_svs PROPERTIES TIMEOUT 3000)

# Guard the per-tier SIMD objects against sharing externally visible symbols, which would let
# link order decide which -march a kernel body was built for. See check_tier_linkage.py.
find_program(PYTHON3_FOR_TESTS NAMES python3 python)
if(PYTHON3_FOR_TESTS)
	add_test(NAME tier_linkage
		COMMAND ${PYTHON3_FOR_TESTS} ${CMAKE_CURRENT_SOURCE_DIR}/check_tier_linkage.py
			$<TARGET_FILE:VectorSimilaritySpaces>)
else()
	message(WARNING "python3 not found, skipping the tier_linkage test")
endif()
