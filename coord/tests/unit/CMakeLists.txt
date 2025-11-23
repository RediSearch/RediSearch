
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)
message("# coord/tests/unit: root: " ${root})

include_directories(${root}/coord/src)
include_directories(${root}/deps)
include_directories(${root}/tests)


# Define all test files
set(UNIT_TESTS
    test_distagg.cpp
    test_shard_window_ratio.c
)

# Create executables for each test
foreach(test_file ${UNIT_TESTS})
    # Extract test name from filename (remove extension)
    get_filename_component(test_name ${test_file} NAME_WE)

    add_executable(${test_name} ${test_file})
    target_link_libraries(${test_name} testdeps m redismock dl)
    set_target_properties(${test_name} PROPERTIES COMPILE_FLAGS "-fvisibility=default")
    target_compile_definitions(${test_name} PRIVATE REDISMODULE_MAIN)
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()
