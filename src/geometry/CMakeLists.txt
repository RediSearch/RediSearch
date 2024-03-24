
set(CMAKE_CXX_STANDARD 20)

# find_package(absl 20230125.1)
file(GLOB SOURCES "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

# include_directories(${absl_INCLUDE_DIR})
if(CANON_BOOST)
	message(STATUS "using find_package")

	find_package(Boost 1.82.0 PATHS ${BOOST_DIR})
	include_directories(${Boost_INCLUDE_DIR})
	if(NOT Boost_FOUND)
		message(FATAL_ERROR "Could not find boost!")
	endif()
endif()

if(NOT BOOST_DIR STREQUAL "")
 	message(STATUS "BOOST_DIR is not empty: ${BOOST_DIR}")
	include_directories(${BOOST_DIR})
else()
 	message(STATUS "BOOST_DIR is not defined")
endif()
