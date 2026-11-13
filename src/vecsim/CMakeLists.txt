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
get_filename_component(binroot ${CMAKE_CURRENT_BINARY_DIR} ABSOLUTE)
message("# VectorSimilarity binroot: " ${binroot})
# Rust's header-change scanner must not descend into standalone build artifacts
# now that the component and its local build directory live below src/.
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

# Only do these if this is the main project, and not if it is included through add_subdirectory
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions -fPIC ${CLANG_SAN_FLAGS} ${LLVM_CXX_FLAGS} ${COV_CXX_FLAGS}")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_LINKER_FLAGS} ${LLVM_LD_FLAGS}")

IF(USE_PROFILE)
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
ENDIF()

include(cmake/svs.cmake)

include_directories(src)

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

	FetchContent_Declare(
		googletest
		URL https://github.com/google/googletest/archive/refs/tags/v1.16.0.zip
	)

	# For Windows: Prevent overriding the parent project's compiler/linker settings
	set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
	FetchContent_MakeAvailable(googletest)

	if(VECSIM_BUILD_TESTS)
		add_subdirectory(tests/unit unit_tests)
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
	add_subdirectory(tests/benchmark benchmark)
endif()

add_subdirectory(src/VecSim)

# Needed for build as ExternalProject (like RediSearch does)
install(TARGETS VectorSimilarity DESTINATION ${CMAKE_INSTALL_PREFIX})
install(TARGETS VectorSimilaritySpaces DESTINATION ${CMAKE_INSTALL_PREFIX})

# Print compiler flags (similar to RediSearch)
get_directory_property(COMPILE_DEFS COMPILE_DEFINITIONS)
message(STATUS "CXXFLAGS:         ${CMAKE_CXX_FLAGS}")
message(STATUS "COMPILE_DEFS:     ${COMPILE_DEFS}")
message(STATUS "LDFLAGS:          ${CMAKE_SHARED_LINKER_FLAGS}")
