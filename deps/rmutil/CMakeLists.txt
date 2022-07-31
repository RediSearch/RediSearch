
set (rmutil_src
    alloc.c
    args.c
    heap.c
    periodic.c
    priority_queue.c
    sds.c
    strings.c
    util.c
    vector.c)

ADD_LIBRARY(rmutil OBJECT ${rmutil_src})

set_source_files_properties(${rmutil_src} PROPERTIES LANGUAGE CXX)

FUNCTION(_rmutilTest name)
    ADD_EXECUTABLE("${name}" "${name}.c" $<TARGET_OBJECTS:rmutil>)
	TARGET_COMPILE_DEFINITIONS("${name}" PRIVATE REDISMODULE_MAIN)
    TARGET_LINK_LIBRARIES(${name} m)
    ADD_TEST(NAME "${name}" COMMAND "${name}")
ENDFUNCTION()

# FILE(GLOB TEST_SOURCES "test_*.c")
# set_source_files_properties(${TEST_SOURCES} PROPERTIES LANGUAGE CXX)
# FOREACH(n ${TEST_SOURCES})
#     GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
#     _rmutilTest("${test_name}")
# ENDFOREACH()

