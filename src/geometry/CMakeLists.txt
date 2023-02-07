set(CMAKE_CXX_STANDARD 20)

file(GLOB SOURCES "*.cpp")

add_library(redisearch-geometry STATIC ${SOURCES})

