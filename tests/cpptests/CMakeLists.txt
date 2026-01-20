
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../.. ABSOLUTE)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${root}/src)
include_directories(${root}/deps)
include_directories(${root}/src/redisearch_rs/headers)
include_directories(.)

if (NOT TEST_MODULE)
  set(TEST_MODULE redisearch)
endif()
# redismock is a mock library for using redis module API in tests, defined in main CMakeLists.txt.

include(GoogleTest)

file(GLOB TEST_SOURCES "test_cpp_*.cpp")
add_executable(rstest ${TEST_SOURCES} common.cpp index_utils.cpp iterator_util.cpp)
target_link_libraries(rstest gtest ${TEST_MODULE} redismock ${CMAKE_LD_LIBS})
set_target_properties(rstest PROPERTIES LINKER_LANGUAGE CXX)
add_dependencies(rstest example_extension)
target_compile_definitions(rstest PRIVATE
    EXT_TEST_PATH="${CMAKE_BINARY_DIR}/example_extension/libexample_extension.so")
gtest_discover_tests(rstest
    PROPERTIES TIMEOUT 300  # Timeout for each individual test
)

add_executable(test_distagg ${root}/tests/cpptests/test_distagg.cpp)
target_link_libraries(test_distagg ${TEST_MODULE} redismock)
set_target_properties(test_distagg PROPERTIES COMPILE_FLAGS "-fvisibility=default")
add_test(name test_distagg COMMAND test_distagg)

# Add the coord_tests subdirectory
add_subdirectory(coord_tests)

file(GLOB BENCHMARK_SOURCES "benchmark_*.cpp")
add_executable(rsbench ${BENCHMARK_SOURCES} index_utils.cpp)
target_link_libraries(rsbench ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} pthread)
set_target_properties(rsbench PROPERTIES LINKER_LANGUAGE CXX)

# Simple VecSim benchmark for comparing backends
add_executable(simple_vecsim_bench simple_vecsim_benchmark.cpp)
target_link_libraries(simple_vecsim_bench ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} pthread)
set_target_properties(simple_vecsim_bench PROPERTIES LINKER_LANGUAGE CXX)
if(USE_RUST_VECSIM)
    target_compile_definitions(simple_vecsim_bench PRIVATE USE_RUST_VECSIM)
endif()
