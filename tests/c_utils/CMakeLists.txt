ADD_LIBRARY(apistubs SHARED api_stubs.c)

IF (RS_RUN_TESTS)
    ADD_EXECUTABLE(sizes sizes.cpp)
    ADD_EXECUTABLE(apibench apibench.cpp)
    TARGET_LINK_LIBRARIES(sizes redisearch apistubs dl)
    TARGET_LINK_LIBRARIES(apibench redisearch redismock apistubs dl)
ENDIF()
