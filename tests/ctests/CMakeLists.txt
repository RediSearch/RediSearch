
if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

function(define_test name)
    add_executable("${name}" "${name}.c")
    target_link_libraries("${name}" ${TEST_MODULE} apistubs ${CMAKE_LD_LIBS})
    set_target_properties("${name}" PROPERTIES LINKER_LANGUAGE CXX)
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    define_test("${test_name}")
endforeach()

# add_subdirectory(ext-example example_extension)
