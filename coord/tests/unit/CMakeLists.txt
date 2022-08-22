
get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)
message("# coord/tests/unit: root: " ${root})

include_directories(${root}/coord/src)
include_directories(${root}/deps)
include_directories(${root}/tests)

add_executable(test_searchcluster test_searchcluster.c)
target_link_libraries(test_searchcluster testdeps m)
set_target_properties(test_searchcluster PROPERTIES COMPILE_FLAGS "-fvisibility=default")
target_compile_definitions(test_searchcluster PRIVATE REDISMODULE_MAIN)

add_executable(test_distagg test_distagg.cpp)
target_link_libraries(test_distagg testdeps m redismock dl)
set_target_properties(test_distagg PROPERTIES COMPILE_FLAGS "-fvisibility=default")
target_compile_definitions(test_distagg PRIVATE REDISMODULE_MAIN)

add_test(NAME test_searchcluster COMMAND test_searchcluster)
add_test(name test_distagg COMMAND test_distagg)
