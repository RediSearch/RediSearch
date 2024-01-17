# Find all the Python files with test_* in them

if (NOT RS_TEST_MODULE)
    set(RS_TEST_MODULE redisearch)
endif()
if (NOT RS_TEST_MODULE_SO)
    set(RS_TEST_MODULE_SO $<TARGET_FILE:${RS_TEST_MODULE}>)
endif()

if (RS_VERBOSE_TESTS)
    list(APPEND RLTEST_ARGS "-s -v")
endif()

file(GLOB PY_TEST_FILES "test*.py")

set(baseCommand "MODARGS+='timeout 0;' RLTEST_ARGS='${RLTEST_ARGS}' ${CMAKE_CURRENT_SOURCE_DIR}/runtests.sh ${RS_TEST_MODULE_SO}")

foreach(n ${PY_TEST_FILES})
    get_filename_component(test_name ${n} NAME_WE)
    add_test(NAME "PY_${test_name}"
        COMMAND bash -c "${baseCommand} -t ${n}"
        WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

#    add_test(NAME "PY_${test_name}_DIALECT_v1"
#		COMMAND bash -c "MODARGS='DEFAULT_DIALECT 1;' ${baseCommand} -t ${n}"
#		WORKING_DIRECTORY ${PROJECT_BINARY_DIR})

    add_test(NAME "PY_${test_name}_DIALECT_v2"
		COMMAND bash -c "MODARGS='DEFAULT_DIALECT 2;' ${baseCommand} -t ${n}"
		WORKING_DIRECTORY ${PROJECT_BINARY_DIR})


endforeach()
