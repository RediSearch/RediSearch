# Find all the Python files with test_* in them
FILE(GLOB PY_TEST_FILES "test*.py")

FOREACH(n ${PY_TEST_FILES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    ADD_TEST(NAME "PY_${test_name}"
        COMMAND "python" "-m" "unittest" "${test_name}"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
    SET_TESTS_PROPERTIES("PY_${test_name}" PROPERTIES ENVIRONMENT
        "REDIS_MODULE_PATH=$<TARGET_FILE:redisearch>;EXT_TEST_PATH=$<TARGET_FILE:example_extension>")

    ADD_TEST(NAME "PY_${test_name}_SAFEMODE"
        COMMAND "python" "-m" "unittest" "${test_name}"
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
    SET_TESTS_PROPERTIES("PY_${test_name}_SAFEMODE" PROPERTIES ENVIRONMENT
        "REDIS_MODULE_PATH=$<TARGET_FILE:redisearch>;EXT_TEST_PATH=$<TARGET_FILE:example_extension>;RS_TEST_SAFEMODE=1")
ENDFOREACH()
