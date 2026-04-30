# Build the `arr` module as a CMake OBJECT library so its objects can be
# pulled into `redisearch_c` via `$<TARGET_OBJECTS:arr>` without producing an
# unused `libarr.a` archive.
file(GLOB ARR_SOURCES "arr.c")
add_library(arr OBJECT ${ARR_SOURCES})
target_include_directories(arr PRIVATE . ../..)
