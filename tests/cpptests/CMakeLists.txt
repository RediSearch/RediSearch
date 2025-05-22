
# include(CTest)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(.)

add_subdirectory(redismock)

if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

# add_definitions(-DEXT_TEST_PATH="\\"${binroot}/search/tests/cpptests/example_extension/libexample_extension.so\\"")

file(GLOB TEST_SOURCES "test_cpp_*.cpp")
add_executable(rstest ${TEST_SOURCES} common.cpp index_utils.cpp)
target_link_libraries(rstest gtest ${TEST_MODULE} ${CMAKE_LD_LIBS} redismock)
set_target_properties(rstest PROPERTIES LINKER_LANGUAGE CXX)

# Only add dependency on example_extension if it's being built
if (BUILD_EXAMPLE_EXTENSION)
    add_dependencies(rstest example_extension)
endif()
add_test(NAME rstest COMMAND rstest)

file(GLOB BENCHMARK_SOURCES "benchmark_*.cpp")
add_executable(rsbench ${BENCHMARK_SOURCES} index_utils.cpp)
target_link_libraries(rsbench ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} pthread)
set_target_properties(rsbench PROPERTIES LINKER_LANGUAGE CXX)
