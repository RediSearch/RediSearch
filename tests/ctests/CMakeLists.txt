include(CTest)

if (NOT RS_TEST_MODULE)
    set(RS_TEST_MODULE redisearch)
endif()

function(RSTEST name)
    add_executable("${name}" "${name}.c")
    target_link_libraries("${name}" ${RS_TEST_MODULE} apistubs ${RS_LINK_LIBS})
	set_target_properties("${name}" PROPERTIES LINKER_LANGUAGE CXX)
    add_test(NAME "${name}" COMMAND "${name}" WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    RSTEST("${test_name}")
endforeach()

# add_subdirectory(ext-example example_extension)
