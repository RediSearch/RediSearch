# Build the `wildcard` module as a CMake OBJECT library so its objects can be
# pulled into `redisearch_c` via `$<TARGET_OBJECTS:wildcard>` without producing
# an unused `libwildcard.a` archive.
file(GLOB WILDCARD_SOURCES "wildcard.c")
add_library(wildcard OBJECT ${WILDCARD_SOURCES})
target_include_directories(wildcard PRIVATE . ..)
