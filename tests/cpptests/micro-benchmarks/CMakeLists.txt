
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

# clang 22+ warns about __COUNTER__ under -Wc2y-extensions: it's a compiler extension
# being standardized in C2y (the C standard after C23). googlebench uses -pedantic-errors
# and -Werror, turning this into a fatal error. __COUNTER__ has been a de facto compiler
# extension so is fine to use it, but we need to suppress the warning now that clang 22+
# is more strict. Earlier clang versions don't know -Wno-c2y-extensions, so we also pass
# -Wno-unknown-warning-option to keep the unknown-flag diagnostic from being fatal there.
target_compile_options(benchmark PRIVATE
  $<$<CXX_COMPILER_ID:Clang>:-Wno-unknown-warning-option -Wno-c2y-extensions>)
target_compile_options(benchmark_main PRIVATE
  $<$<CXX_COMPILER_ID:Clang>:-Wno-unknown-warning-option -Wno-c2y-extensions>)

file(GLOB BENCHMARK_ITER_SOURCES "benchmark_*_iterator.cpp")
foreach(benchmark_file ${BENCHMARK_ITER_SOURCES})
  get_filename_component(benchmark_name ${benchmark_file} NAME_WE)
  add_executable(${benchmark_name} ${benchmark_file} ../index_utils.cpp ../iterator_util.cpp)
  target_link_libraries(${benchmark_name} redisearch redismock benchmark::benchmark)
endforeach()
