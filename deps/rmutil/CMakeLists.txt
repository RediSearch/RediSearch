add_library(rmutil OBJECT
    alloc.c
    args.c
    cmdparse.c
    heap.c
    priority_queue.c
	strings.c
    util.c
    vector.c)

if (RMUTIL_TESTS OR BUILD_SEARCH_UNIT_TESTS)
	function(_rmutilTest name)
		add_executable(${name} "${name}.c" $<TARGET_OBJECTS:rmutil>)
		target_compile_definitions(${name} PRIVATE REDISMODULE_MAIN)
		# libm: the rmutil objects (cmdparse.c) use round()
		target_link_libraries(${name} ${CMAKE_LD_LIBS} m)
		# The unit-test runner discovers C test binaries by scanning
		# <build>/tests/ctests for test_* executables.
		set_target_properties(${name} PROPERTIES
			RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests/ctests")
		add_test(NAME "${name}" COMMAND "${name}")
	endfunction()

	# Only test_args is maintained and part of the unit-test run; the other
	# test_*.c files here predate the module and are opt-in via RMUTIL_TESTS.
	_rmutilTest(test_args)
	if (RMUTIL_TESTS)
		file(GLOB TEST_SOURCES "test_*.c")
		list(REMOVE_ITEM TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/test_args.c")
		foreach(n ${TEST_SOURCES})
			get_filename_component(test_name ${n} NAME_WE)
			_rmutilTest("${test_name}")
		endforeach()
	endif()
endif()
