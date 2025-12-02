file(GLOB SOURCES "value.c")
add_library(value STATIC ${SOURCES})
target_include_directories(value PRIVATE . ..)
