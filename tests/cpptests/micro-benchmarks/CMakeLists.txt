
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${root}/src)
include_directories(${root}/deps)
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

if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

file(GLOB BENCHMARK_SOURCES "benchmark_*_iterator.cpp")
add_executable(micro_bench_iterators ${BENCHMARK_SOURCES} ../index_utils.cpp )
target_link_libraries(micro_bench_iterators ${TEST_MODULE} redismock benchmark::benchmark)
