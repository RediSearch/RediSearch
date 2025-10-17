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

if (NOT TEST_MODULE)
  set(TEST_MODULE redisearch)
endif()

file(GLOB TEST_SOURCES "test_cpp_*.cpp")

set(COMMON_FILES
  ../common.cpp
  ../index_utils.cpp
)

add_executable(rstest_coord ${TEST_SOURCES} ${COMMON_FILES})
target_link_libraries(rstest_coord gtest ${TEST_MODULE} redismock ${CMAKE_LD_LIBS})
set_target_properties(rstest_coord PROPERTIES LINKER_LANGUAGE CXX)
add_test(NAME rstest_coord COMMAND rstest_coord)
