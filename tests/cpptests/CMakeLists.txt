
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
# set_property(TARGET rstest PROPERTY CXX_STANDARD 11)
add_dependencies(rstest example_extension)
add_test(NAME rstest COMMAND rstest)
# set_tests_properties(rstest PROPERTIES ENVIRONMENT "EXT_TEST_PATH=$<TARGET_FILE:example_extension>")

file(GLOB BENCHMARK_SOURCES "benchmark_*.cpp")
add_executable(rsbench ${BENCHMARK_SOURCES} index_utils.cpp)
target_link_libraries(rsbench ${TEST_MODULE} redismock ${CMAKE_LD_LIBS} pthread)
set_target_properties(rsbench PROPERTIES LINKER_LANGUAGE CXX)
# set_property(TARGET rsbench PROPERTY CXX_STANDARD 11)
