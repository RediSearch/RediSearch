set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

get_filename_component(root ${CMAKE_CURRENT_LIST_DIR}/../../.. ABSOLUTE)

include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${root}/src)
include_directories(${root}/deps)
include_directories(${root}/src/coord/rmr)
include_directories(${root}/src/coord/hybrid)
include_directories(..)
include_directories(.)

# Add VecSim include path based on which implementation is used
if(USE_RUST_VECSIM)
    # Rust VecSim headers - use BEFORE to ensure precedence
    include_directories(BEFORE ${VECSIM_C_INCLUDE_DIR})
else()
    # C++ VectorSimilarity headers
    include_directories(${root}/deps/VectorSimilarity/src)
endif()

if (NOT TEST_MODULE)
  set(TEST_MODULE redisearch)
endif()

include(GoogleTest)

file(GLOB TEST_SOURCES "test_cpp_*.cpp")

set(COMMON_FILES
  ../common.cpp
  ../index_utils.cpp
  ../stacktrace.cpp
)

add_executable(rstest_coord ${TEST_SOURCES} ${COMMON_FILES})
target_link_libraries(rstest_coord gtest ${TEST_MODULE} redismock ${CMAKE_LD_LIBS})
set_target_properties(rstest_coord PROPERTIES LINKER_LANGUAGE CXX)
gtest_discover_tests(rstest_coord
    PROPERTIES TIMEOUT 300  # Timeout for each individual test
)
