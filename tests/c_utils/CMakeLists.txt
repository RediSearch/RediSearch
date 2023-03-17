add_library(apistubs STATIC api_stubs.c)

if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

if (BUILD_TESTS)
    add_executable(sizes sizes.cpp)
    target_compile_definitions(sizes PRIVATE REDISMODULE_MAIN)
    target_link_libraries(sizes ${TEST_MODULE} apistubs ${CMAKE_LD_LIBS})

    add_executable(apibench apibench.cpp)
    target_compile_definitions(apibench PRIVATE REDISMODULE_MAIN)
    target_link_libraries(apibench ${TEST_MODULE} redismock apistubs ${CMAKE_LD_LIBS})
endif()
