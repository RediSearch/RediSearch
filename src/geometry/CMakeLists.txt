set(CMAKE_CXX_STANDARD 20)

include(${CONAN_BINDIR}/absl-config.cmake)

file(GLOB SOURCES
    "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

include_directories(
	${BOOST_DIR}
	${absl_INCLUDE_DIR})

