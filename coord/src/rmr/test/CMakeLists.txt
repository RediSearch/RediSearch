
get_filename_component(root ${CMAKE_CURRENT_SOURCE_DIR}/../../../.. ABSOLUTE)
message("# coord/rmr/test: root: " ${root})

include_directories(..)

function(RMRTEST name)
    add_executable(${name} ${name}.c init-rm.c)

    target_link_libraries("${name}" testdeps redismock ${CMAKE_LD_LIBS})
    add_test(NAME "${name}" COMMAND "${name}")
    set_target_properties("${name}" PROPERTIES COMPILE_FLAGS "-fvisibility=default")
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    RMRTEST(${test_name})
endforeach()
