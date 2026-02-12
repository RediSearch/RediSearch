file(GLOB SOURCES "query_term.c")
add_library(query_term STATIC ${SOURCES})
target_include_directories(query_term PRIVATE . ..)
