
<<<<<<<< HEAD:tests/ctests/coord_tests/CMakeLists.txt
if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

include_directories(${root}/src/coord/rmr)
add_compile_definitions(RMUTIL_NO_SDS)
========
get_filename_component(root ../../../.. ABSOLUTE)
message("# coord/rmr/test: root: " ${root})

include_directories(${root}/src/coord/rmr)
>>>>>>>> master:src/coord/rmr/test/CMakeLists.txt

function(RMRTEST name)
    add_executable(${name} ${name}.c init-rm.c)
    add_dependencies(${name} ${TEST_MODULE} testdeps)
    target_link_libraries("${name}" testdeps ${TEST_MODULE} ${CMAKE_LD_LIBS})
    add_test(NAME "${name}" COMMAND "${name}")
    set_target_properties("${name}" PROPERTIES COMPILE_FLAGS "-fvisibility=default")
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    RMRTEST(${test_name})
endforeach()
