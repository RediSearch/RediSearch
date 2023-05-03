
set(CMAKE_CXX_STANDARD 20)

include(${CONAN_BINDIR}/conan_toolchain.cmake)

find_package(absl 20230125.1)
find_package(Boost 1.69.0)

file(GLOB SOURCES
    "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

target_compile_features(redisearch-geometry PRIVATE cxx_std_20)

include_directories(
	${BOOST_DIR}
	${absl_INCLUDE_DIR}
	${Boost_INCLUDE_DIRS})
