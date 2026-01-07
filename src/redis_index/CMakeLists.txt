# Build the `redis_index` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB REDIS_INDEX_SOURCES "*.c")
add_library(redis_index STATIC ${REDIS_INDEX_SOURCES})
target_include_directories(redis_index PRIVATE . ..)
