# C++ tests for RedisearchDisk, modeled after RediSearch tests/cpptests

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Root of this repo and RediSearch submodule
set(ROOT_DIR ${PROJECT_SOURCE_DIR})
set(REDISEARCH_DIR ${PROJECT_SOURCE_DIR}/deps/RediSearch)

# Bring GoogleTest from RediSearch submodule
add_subdirectory(${REDISEARCH_DIR}/deps/googletest ${CMAKE_BINARY_DIR}/googletest)

# Include dirs similar to RediSearch tests
include_directories("${gtest_SOURCE_DIR}/include")
include_directories(${REDISEARCH_DIR}/src)
include_directories(${REDISEARCH_DIR}/src/buffer) # for buffer.h used by varint.h
include_directories(${REDISEARCH_DIR}/src/inverted_index) # for inverted_index.h in numeric_index.h
include_directories(${REDISEARCH_DIR}/deps)
include_directories(${REDISEARCH_DIR}/deps/VectorSimilarity/src)
include_directories(${REDISEARCH_DIR}/deps/RedisModulesSDK)
include_directories(${REDISEARCH_DIR}/src/redisearch_rs/headers)
include_directories(${REDISEARCH_DIR}/tests/cpptests) # for redismock headers
include_directories(${REDISEARCH_DIR}/.install/boost) # boost headers used by redismock
include_directories(${ROOT_DIR})
include_directories(${ROOT_DIR}/deps/speedb-ent/include)
include_directories(${ROOT_DIR}/deps/CRoaring/include)
include_directories(${ROOT_DIR}/deps/CRoaring) # for cpp/roaring/roaring64map.hh

# Make sure redismock is available (from RediSearch)
add_subdirectory(${REDISEARCH_DIR}/tests/cpptests/redismock ${CMAKE_BINARY_DIR}/redismock)

# Unless TEST_MODULE was defined explicitly to redisearch-static, set it to redisearch
if (NOT TEST_MODULE)
  set(TEST_MODULE redisearch)
endif()

# Collect our test sources
file(GLOB TEST_SOURCES "test_cpp_*.cpp")
set(TEST_SUPPORT ${REDISEARCH_DIR}/tests/cpptests/common.cpp)

add_executable(rsdisk_cpptests ${TEST_SOURCES} ${TEST_SUPPORT})

# Find liburing if available
find_package(uring)

# Link against gtest and gtest_main, the redisearch static library, our disk library, and redismock
if (TARGET gtest AND TARGET gtest_main)
  target_link_libraries(rsdisk_cpptests PRIVATE gtest gtest_main ${TEST_MODULE} redismock redisearch-disk
    ${CMAKE_BINARY_DIR}/speedb-build/libspeedb.a
    ${CMAKE_BINARY_DIR}/croaring-build/src/libroaring.a
    ${CMAKE_LD_LIBS} pthread
    $<IF:$<BOOL:${uring_FOUND}>,uring::uring,uring>)
else()
  message(FATAL_ERROR "gtest targets not found. Make sure googletest was added via add_subdirectory.")
endif()

set_target_properties(rsdisk_cpptests PROPERTIES LINKER_LANGUAGE CXX)

# Discover and register each GoogleTest as an individual CTest test
# Match RediSearch user experience: colorized output and serialized execution
include(GoogleTest)
gtest_discover_tests(rsdisk_cpptests
  DISCOVERY_MODE PRE_TEST
  EXTRA_ARGS --gtest_color=yes
  PROPERTIES
    ENVIRONMENT "GTEST_COLOR=1"
    RUN_SERIAL TRUE
)

