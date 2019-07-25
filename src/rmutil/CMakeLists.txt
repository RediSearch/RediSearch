ADD_LIBRARY(rmutil OBJECT
    alloc.c
    args.c
    heap.c
    periodic.c
    priority_queue.c
    sds.c
    strings.c
    util.c
    vector.c)

FUNCTION(_rmutilTest name)
    ADD_EXECUTABLE("${name}" "${name}.c" $<TARGET_OBJECTS:rmutil>)
    TARGET_LINK_LIBRARIES(${name} m)
    ADD_TEST(NAME "${name}" COMMAND "${name}")
ENDFUNCTION()

FILE(GLOB TEST_SOURCES "test_*.c")
FOREACH(n ${TEST_SOURCES})
    GET_FILENAME_COMPONENT(test_name ${n} NAME_WE)
    _rmutilTest("${test_name}")
ENDFOREACH()

