INCLUDE (CTest)

FUNCTION(RSTEST name)
    ADD_EXECUTABLE("${name}" "${name}.c")
    TARGET_LINK_LIBRARIES("${name}" "redisearchS")
    ADD_TEST(NAME "${name}" COMMAND "${name}" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
ENDFUNCTION()

FILE(GLOB TEST_SOURCES "test_*.c")

FOREACH(n ${TEST_SOURCES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    RSTEST("${test_name}")
ENDFOREACH()

ADD_LIBRARY(example_extension SHARED "ext-example/example.c")
ADD_DEPENDENCIES(test_extensions example_extension)
SET_TESTS_PROPERTIES(test_extensions PROPERTIES
    ENVIRONMENT "EXT_TEST_PATH=$<TARGET_FILE:example_extension>"
)
