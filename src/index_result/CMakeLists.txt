file(GLOB SOURCES "index_result.c")
add_library(index_result STATIC ${SOURCES})
target_include_directories(index_result PRIVATE . ..)
