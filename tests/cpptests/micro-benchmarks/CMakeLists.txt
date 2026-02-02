
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/micro-benchmarks)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${root}/src)
include_directories(${root}/deps)
include_directories(${root}/src/redisearch_rs/headers)
include_directories(..)
include_directories(.)

# Add VecSim include path based on which implementation is used
if(USE_RUST_VECSIM)
    # Rust VecSim headers - use BEFORE to ensure precedence
    include_directories(BEFORE ${VECSIM_C_INCLUDE_DIR})
else()
    # C++ VectorSimilarity headers
    include_directories(${root}/deps/VectorSimilarity/src)
endif()

include(FetchContent)
FetchContent_Declare(
    googlebench
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.9.1
)
set(BENCHMARK_ENABLE_TESTING OFF)
FetchContent_MakeAvailable(googlebench)
include_directories("${googlebench_SOURCE_DIR}/include")

file(GLOB BENCHMARK_ITER_SOURCES "benchmark_*_iterator.cpp")
foreach(benchmark_file ${BENCHMARK_ITER_SOURCES})
  get_filename_component(benchmark_name ${benchmark_file} NAME_WE)
  add_executable(${benchmark_name} ${benchmark_file} ../index_utils.cpp ../iterator_util.cpp)
  target_link_libraries(${benchmark_name} redisearch redismock benchmark::benchmark)
endforeach()
