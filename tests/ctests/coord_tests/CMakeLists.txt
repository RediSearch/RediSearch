
if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

include_directories(${root}/src/coord/rmr)

function(RMRTEST name)
    add_executable(${name} ${name}.c)
    add_dependencies(${name} ${TEST_MODULE} redismock)
    target_link_libraries("${name}" redismock ${TEST_MODULE} ${CMAKE_LD_LIBS})
    add_test(NAME "${name}" COMMAND "${name}")
    set_target_properties("${name}" PROPERTIES COMPILE_FLAGS "-fvisibility=default")
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    RMRTEST(${test_name})
endforeach()
