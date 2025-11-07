
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

# Add the SharedExclusiveLock vs Mutex benchmark
add_executable(benchmark_shared_exclusive_lock_vs_regular_lock benchmark_shared_exclusive_lock_vs_regular_lock.cpp)
target_link_libraries(benchmark_shared_exclusive_lock_vs_regular_lock redisearch redismock benchmark::benchmark)
