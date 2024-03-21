
set(CMAKE_CXX_STANDARD 20)

# find_package(absl 20230125.1)
file(GLOB SOURCES "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

# include_directories(${absl_INCLUDE_DIR})

if(NOT BOOST_DIR STREQUAL "")
 	message(STATUS "BOOST_DIR is not empty: ${BOOST_DIR}")
	include_directories(${BOOST_DIR})
else()
 	message(STATUS "BOOST_DIR is empty: ${BOOST_DIR}")
endif()
