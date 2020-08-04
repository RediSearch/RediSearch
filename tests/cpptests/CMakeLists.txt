INCLUDE(CTest)

INCLUDE_DIRECTORIES("${gtest_SOURCE_DIR}/include")
INCLUDE_DIRECTORIES(.)
ADD_SUBDIRECTORY(redismock)

FUNCTION(RSTEST name)
    ADD_EXECUTABLE("${name}" "${name}.cpp" common.cpp)
    TARGET_LINK_LIBRARIES("${name}" gtest redisearch redismock)
    SET_PROPERTY(TARGET "${name}" PROPERTY CXX_STANDARD 11)
    ADD_DEPENDENCIES("${name}" example_extension)
    ADD_TEST(NAME "${name}" COMMAND "${name}" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
    SET_TESTS_PROPERTIES("${name}" PROPERTIES
        ENVIRONMENT "EXT_TEST_PATH=$<TARGET_FILE:example_extension>"
    )
    ADD_DEFINITIONS(-DEXT_TEST_PATH="$<TARGET_FILE:example_extension>")
ENDFUNCTION()

FILE(GLOB TEST_SOURCES "t_*.cpp")

FOREACH(n ${TEST_SOURCES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    RSTEST("${test_name}")
ENDFOREACH()