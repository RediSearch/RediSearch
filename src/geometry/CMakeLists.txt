set(CMAKE_CXX_STANDARD 20)

file(GLOB SOURCES
    "*.cpp"
    "geometry_api.c")

add_library(redisearch-geometry STATIC ${SOURCES})

