add_library(apistubs SHARED api_stubs.c)

if (NOT RS_TEST_MODULE)
    set(RS_TEST_MODULE redisearch)
endif()

if (RS_RUN_TESTS)
    add_executable(sizes sizes.cpp)
    target_compile_definitions(sizes PRIVATE REDISMODULE_MAIN)
    target_link_libraries(sizes ${RS_TEST_MODULE} apistubs ${RS_LINK_LIBS})
	
    add_executable(apibench apibench.cpp)
    target_compile_definitions(apibench PRIVATE REDISMODULE_MAIN)
    target_link_libraries(apibench ${RS_TEST_MODULE} redismock apistubs ${RS_LINK_LIBS})
endif()
