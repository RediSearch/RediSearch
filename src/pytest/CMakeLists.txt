# Find all the Python files with test_* in them

IF (NOT RS_TEST_MODULE_SO)
    SET(RS_TEST_MODULE_SO $<TARGET_FILE:redisearch>)
ENDIF()
IF (RS_VERBOSE_TESTS)
    LIST(APPEND RS_TEST_ARGS "-s")
ENDIF()

FILE(GLOB PY_TEST_FILES "test*.py")
SET(baseCommand "${CMAKE_CURRENT_SOURCE_DIR}/runtests.sh;--module=${RS_TEST_MODULE_SO};${RS_TEST_ARGS}")

FOREACH(n ${PY_TEST_FILES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    ADD_TEST(NAME "PY_${test_name}"
        COMMAND ${baseCommand} "-t" "${n}"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_SAFEMODE"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=SAFEMODE"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_legacygc"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=GC_POLICY LEGACY"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_CRITERIA_TESTER"
        COMMAND ${baseCommand} "-t" "${n}" "--module-args=_MAX_RESULTS_TO_UNSORTED_MODE 1"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
ENDFOREACH()
