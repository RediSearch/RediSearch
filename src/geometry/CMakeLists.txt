
set(CMAKE_CXX_STANDARD 20)

# find_package(absl 20230125.1)

file(GLOB SOURCES "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

# include_directories(${absl_INCLUDE_DIR})

message(STATUS "The value of BOOST_DIR is: ${BOOST_DIR}")
include_directories(${BOOST_DIR})
