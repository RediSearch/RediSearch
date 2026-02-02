
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

include_directories(${root}/src/coord/rmr)
include_directories(${root}/src/coord/hybrid)

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
