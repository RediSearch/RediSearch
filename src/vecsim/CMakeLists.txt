cmake_minimum_required(VERSION 3.10)
cmake_policy(SET CMP0077 NEW)

set(CMAKE_CXX_STANDARD 20)

# VECSIM_BUILD_TEST_HELPERS: Enables test helper methods (fitMemory, serialization, etc.)
# in VectorSimilarity classes without building test executables.
option(VECSIM_BUILD_TEST_HELPERS "Build test helper methods (fitMemory, serialization)" OFF)

# VECSIM_BUILD_TESTS: Builds VectorSimilarity unit test executables.
# When ON, test helpers are also built even if VECSIM_BUILD_TEST_HELPERS is OFF.
option(VECSIM_BUILD_TESTS "Build VectorSimilarity unit tests" ON)
option(VECSIM_BUILD_BENCHMARKS "Build VectorSimilarity benchmarks" OFF)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR} ABSOLUTE)
message("# VectorSimilarity root: " ${root})
get_filename_component(SEARCH_ROOT "${root}/../.." ABSOLUTE)

get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR} ABSOLUTE)
message("# VectorSimilarity binroot: " ${binroot})
# Mark standalone build output as a cache for source scanners.
file(WRITE "${binroot}/CACHEDIR.TAG" "Signature: 8a477f597d28d172789f06886806bc55\n")

if(USE_COVERAGE)
	if(NOT CMAKE_BUILD_TYPE STREQUAL "DEBUG")
		message(FATAL_ERROR "Build type must be DEBUG for coverage")
	endif()

	set(COV_CXX_FLAGS "-coverage")
endif()

include(cmake/san.cmake)

# ----------------------------------------------------------------------------------------------
project(VectorSimilarity)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	find_program(VECSIM_SCCACHE sccache)
	if(VECSIM_SCCACHE)
		set(CMAKE_C_COMPILER_LAUNCHER ${VECSIM_SCCACHE})
		set(CMAKE_CXX_COMPILER_LAUNCHER ${VECSIM_SCCACHE})
	endif()
endif()

# Only do these if this is the main project, and not if it is included through add_subdirectory
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions -fPIC ${CLANG_SAN_FLAGS} ${LLVM_CXX_FLAGS} ${COV_CXX_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")

IF(USE_PROFILE)
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
ENDIF()

include(cmake/svs.cmake)
if(TARGET fmt)
	# fmt 11 lacks a cstdlib include required by clang 21 on macOS.
	target_compile_options(fmt PRIVATE "-include" "cstdlib")
endif()

include_directories(${SEARCH_ROOT}/src)

# Define BUILD_TESTS macro if test helpers OR tests are requested.
# This enables test helper methods (fitMemory, serialization, etc.) in class definitions.
if(VECSIM_BUILD_TEST_HELPERS OR VECSIM_BUILD_TESTS OR VECSIM_BUILD_BENCHMARKS)
	ADD_DEFINITIONS(-DBUILD_TESTS)
endif()

# Unit tests and benchmarks share GoogleTest, but ordinary module builds need neither.
if(VECSIM_BUILD_TESTS OR VECSIM_BUILD_BENCHMARKS)
	include(FetchContent)
	enable_testing()

	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")

	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions -fPIC ${CLANG_SAN_FLAGS} ${LLVM_CXX_FLAGS} ${COV_CXX_FLAGS}")
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")
	set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")

	if(NOT TARGET gtest)
		add_subdirectory(${SEARCH_ROOT}/deps/googletest ${binroot}/googletest)
	endif()

	if(VECSIM_BUILD_TESTS)
		add_subdirectory(${SEARCH_ROOT}/tests/cpptests/vecsim/unit unit_tests)
	endif()
endif()

if(VECSIM_BUILD_BENCHMARKS)
	include(FetchContent)
	option(BENCHMARK_ENABLE_GTEST_TESTS "" OFF)
	option(BENCHMARK_ENABLE_TESTING "" OFF)
	FetchContent_Declare(
		google_benchmark
		URL https://github.com/google/benchmark/archive/refs/tags/v1.8.0.zip
	)
	FetchContent_MakeAvailable(google_benchmark)
	add_subdirectory(${SEARCH_ROOT}/tests/cpptests/micro-benchmarks/vecsim benchmark)
endif()

file(GLOB_RECURSE headers
    algorithms/*.h containers/*.h index_factories/*.h memory/*.h quantization/*.h spaces/*.h types/*.h utils/*.h)
file(GLOB public_headers *.h)
list(APPEND headers ${public_headers})
set(HEADER_LIST "${headers}")

include_directories(../)

set(SVS_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++20")

add_subdirectory(spaces)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Wall")

if(USE_SVS)
    set(svs_factory_file "index_factories/svs_factory.cpp")
endif()

add_library(VectorSimilarity ${VECSIM_LIBTYPE}
    index_factories/brute_force_factory.cpp
    index_factories/hnsw_factory.cpp
    index_factories/tiered_factory.cpp
    index_factories/svs_factory.cpp
    index_factories/index_factory.cpp
    algorithms/hnsw/visited_nodes_handler.cpp
    vec_sim.cpp
    vec_sim_debug.cpp
    vec_sim_interface.cpp
    query_results.cpp
    info_iterator.cpp
    utils/vec_utils.cpp
    containers/data_block.cpp
    containers/data_blocks_container.cpp
    memory/vecsim_malloc.cpp
    memory/vecsim_base.cpp
    ${HEADER_LIST}
)

target_link_libraries(VectorSimilarity VectorSimilaritySpaces)

if (TARGET svs::svs)
    target_link_libraries(VectorSimilarity svs::svs)
    if(TARGET svs::svs_static_library)
        target_link_libraries(VectorSimilarity svs::svs_static_library)
    endif()
endif()

# The serializer is needed by the BUILD_TESTS class definitions.
if(VECSIM_BUILD_TEST_HELPERS OR VECSIM_BUILD_TESTS OR VECSIM_BUILD_BENCHMARKS)
    add_library(VectorSimilaritySerializer
        algorithms/hnsw/hnsw_serializer.cpp
        algorithms/svs/svs_serializer.cpp
    )
    target_link_libraries(VectorSimilarity VectorSimilaritySerializer)
endif()

# Needed for build as ExternalProject (like RediSearch does)
install(TARGETS VectorSimilarity DESTINATION ${CMAKE_INSTALL_PREFIX})
install(TARGETS VectorSimilaritySpaces DESTINATION ${CMAKE_INSTALL_PREFIX})

# Print compiler flags (similar to RediSearch)
get_directory_property(COMPILE_DEFS COMPILE_DEFINITIONS)
message(STATUS "CXXFLAGS:         ${CMAKE_CXX_FLAGS}")
message(STATUS "COMPILE_DEFS:     ${COMPILE_DEFS}")
message(STATUS "LDFLAGS:          ${CMAKE_SHARED_LINKER_FLAGS}")
