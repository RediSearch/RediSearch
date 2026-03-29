# Build the `wildcard` module as an object library for inclusion in libredisearch_all.a
# This is required because the main CMakeLists.txt uses $<TARGET_OBJECTS:wildcard>
file(GLOB ARR_SOURCES "wildcard.c")
add_library(wildcard OBJECT ${ARR_SOURCES})
target_include_directories(wildcard PRIVATE . ..)
