
# Unless TEST_MODULE was defined explicitly to redisearch-static, set it to redisearch
if (NOT TEST_MODULE)
    set(TEST_MODULE redisearch)
endif()

# Add VecSim include path based on which implementation is used
if(USE_RUST_VECSIM)
    # Rust VecSim headers - use BEFORE to ensure precedence
    include_directories(BEFORE ${VECSIM_C_INCLUDE_DIR})
else()
    # C++ VectorSimilarity headers
    include_directories(${root}/deps/VectorSimilarity/src)
endif()

function(define_test name)
    add_executable("${name}" "${name}.c")
    target_link_libraries("${name}" ${TEST_MODULE} ${CMAKE_LD_LIBS})
    set_target_properties("${name}" PROPERTIES LINKER_LANGUAGE CXX)
endfunction()

file(GLOB TEST_SOURCES "test_*.c")

foreach(n ${TEST_SOURCES})
    get_filename_component(test_name ${n} NAME_WE)
    define_test("${test_name}")
endforeach()
