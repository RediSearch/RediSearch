
include(CTest)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(.)

add_subdirectory(redismock)

if (NOT RS_TEST_MODULE)
    set(RS_TEST_MODULE redisearch)
endif()

file(GLOB TEST_SOURCES "test_cpp_*.cpp")

# add_definitions(-DEXT_TEST_PATH="\\"${binroot}/search/tests/cpptests/example_extension/libexample_extension.so\\"")

add_executable(rstest ${TEST_SOURCES} common.cpp)
target_link_libraries(rstest gtest ${RS_TEST_MODULE} redismock dl)
set_target_properties(rstest PROPERTIES LINKER_LANGUAGE CXX)
set_property(TARGET rstest PROPERTY CXX_STANDARD 11)
add_dependencies(rstest example_extension)

add_test(NAME rstest COMMAND rstest)
# set_tests_properties(rstest PROPERTIES ENVIRONMENT "EXT_TEST_PATH=$<TARGET_FILE:example_extension>")
