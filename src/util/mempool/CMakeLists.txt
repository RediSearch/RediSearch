file(GLOB SOURCES "mempool.c")
add_library(mempool STATIC ${SOURCES})
target_include_directories(mempool PRIVATE . ..)
