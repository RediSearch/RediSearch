
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../.. ABSOLUTE)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${root}/src)
include_directories(${root}/deps)
include_directories(.)

if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()
# redismock is a mock library for using redis module API in tests, defined in main CMakeLists.txt.

file(GLOB TEST_SOURCES "test_cpp_*.cpp")
add_executable(rstest ${TEST_SOURCES} common.cpp index_utils.cpp)
target_link_libraries(rstest gtest ${TEST_MODULE} redismock ${CMAKE_LD_LIBS})
set_target_properties(rstest PROPERTIES LINKER_LANGUAGE CXX)
add_dependencies(rstest example_extension)
add_test(NAME rstest COMMAND rstest)


add_executable(test_distagg ${root}/tests/cpptests/test_distagg.cpp)
target_link_libraries(test_distagg ${TEST_MODULE} redismock)
set_target_properties(test_distagg PROPERTIES COMPILE_FLAGS "-fvisibility=default")
add_test(name test_distagg COMMAND test_distagg)


file(GLOB BENCHMARK_SOURCES "benchmark_*.cpp")
add_executable(rsbench ${BENCHMARK_SOURCES} index_utils.cpp)
target_link_libraries(rsbench ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} pthread)
set_target_properties(rsbench PROPERTIES LINKER_LANGUAGE CXX)


enable_testing()
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

find_package(PkgConfig REQUIRED)
pkg_check_modules(ROCKSDB REQUIRED rocksdb)

include_directories(${ROCKSDB_INCLUDE_DIRS})
link_directories(${ROCKSDB_LIBRARY_DIRS})
add_definitions(${ROCKSDB_CFLAGS_OTHER})

include(FetchContent)
option(BENCHMARK_ENABLE_GTEST_TESTS "" OFF)
option(BENCHMARK_ENABLE_TESTING "" OFF)
FetchContent_Declare(
	google_benchmark
	URL https://github.com/google/benchmark/archive/refs/tags/v1.9.1.zip
)
FetchContent_MakeAvailable(google_benchmark)

add_executable(bm_disk bm_disk.cpp DocTable.cpp inverted_index.cpp)
target_link_libraries(bm_disk ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} benchmark::benchmark ${ROCKSDB_LIBRARIES})
