# Find all the Python files with test_* in them
FILE(GLOB PY_TEST_FILES "test*.py")
SET(baseCommand "${CMAKE_CURRENT_SOURCE_DIR}/runtests.sh;--module=$<TARGET_FILE:redisearch>")

FOREACH(n ${PY_TEST_FILES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    ADD_TEST(NAME "PY_${test_name}"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=GC_POLICY DEFAULT"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_SAFEMODE"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=SAFEMODE GC_POLICY DEFAULT"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_FORKGC"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=GC_POLICY FORK"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_CRITERIA_TESTER"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=GC_POLICY DEFAULT _MAX_RESULTS_TO_UNSORTED_MODE 1"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
ENDFOREACH()
