ADD_LIBRARY(apistubs SHARED api_stubs.c)

IF (NOT RS_TEST_MODULE_SO)
    SET(RS_TEST_MODULE_SO $<TARGET_FILE:redisearch>)
ENDIF()

IF (RS_RUN_TESTS)
    ADD_EXECUTABLE(sizes sizes.cpp)
    ADD_EXECUTABLE(apibench apibench.cpp)
    TARGET_LINK_LIBRARIES(sizes ${RS_TEST_MODULE_SO} apistubs dl)
    TARGET_LINK_LIBRARIES(apibench ${RS_TEST_MODULE_SO} redismock apistubs dl)
ENDIF()
