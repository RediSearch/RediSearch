add_library(rmutil OBJECT
    alloc.c
    args.c
    cmdparse.c
    heap.c
    periodic.c
    priority_queue.c
    sds.c
    strings.c
    util.c
    vector.c)

if(RMUTIL_NO_SDS)
	target_compile_definitions(rmutil PRIVATE RMUTIL_NO_SDS)
endif()

if (RMUTIL_TESTS)
	function(_rmutilTest name)
		add_executable(${name} "${name}.c" $<TARGET_OBJECTS:rmutil>)
		target_compile_definitions(${name} PRIVATE REDISMODULE_MAIN)
		target_link_libraries(${name} ${CMAKE_LD_LIBS})
		add_test(NAME "${name}" COMMAND "${name}")
	endfunction()

	file(GLOB TEST_SOURCES "test_*.c")
	foreach(n ${TEST_SOURCES})
		get_filename_component(test_name ${n} NAME_WE)
		_rmutilTest("${test_name}")
	endforeach()
endif()
