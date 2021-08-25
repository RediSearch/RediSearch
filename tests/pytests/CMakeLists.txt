# Find all the Python files with test_* in them

IF (NOT RS_TEST_MODULE)
    SET(RS_TEST_MODULE redisearch)
ENDIF()
IF (NOT RS_TEST_MODULE_SO)
    SET(RS_TEST_MODULE_SO $<TARGET_FILE:${RS_TEST_MODULE}>)
ENDIF()

IF (RS_VERBOSE_TESTS)
    LIST(APPEND RLTEST_ARGS "-s -v")
ENDIF()

FILE(GLOB PY_TEST_FILES "test*.py")

SET(baseCommand "MODARGS+='timeout 0;' RLTEST_ARGS='${RLTEST_ARGS}' ${CMAKE_CURRENT_SOURCE_DIR}/runtests.sh ${RS_TEST_MODULE_SO}")

FOREACH(n ${PY_TEST_FILES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    ADD_TEST(NAME "PY_${test_name}"
        COMMAND bash -c "${baseCommand} -t ${n}"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_CONCURRENT_WRITE_MODE"
        COMMAND bash -c "MODARGS='CONCURRENT_WRITE_MODE;' ${baseCommand} -t ${n}"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

#    ADD_TEST(NAME "PY_${test_name}_legacygc"
#        COMMAND bash -c "MODARGS='GC_POLICY LEGACY' ${baseCommand} -t ${n}"
#        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_CRITERIA_TESTER"
        COMMAND bash -c "MODARGS='_MAX_RESULTS_TO_UNSORTED_MODE 1;' ${baseCommand} -t ${n}"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    ADD_TEST(NAME "PY_${test_name}_UNION_ITERATOR_HEAP"
    COMMAND bash -c "MODARGS='UNION_ITERATOR_HEAP 1;' ${baseCommand} -t ${n}"
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR})
ENDFOREACH()
