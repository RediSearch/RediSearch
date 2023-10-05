
set(CMAKE_CXX_STANDARD 20)

include(${CONAN_BINDIR}/conan_toolchain.cmake)

# find_package(absl 20230125.1)
if(CANON_BOOST)
	find_package(Boost 1.82.0)
endif()

file(GLOB SOURCES "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

# include_directories(${absl_INCLUDE_DIR})

if(CANON_BOOST)
	include_directories(${Boost_INCLUDE_DIRS})
else()
	include_directories(${BOOST_DIR})
endif()
