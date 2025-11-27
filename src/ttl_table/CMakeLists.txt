# Build the `ttl_table` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB TTL_TABLE_SOURCES "*.c")
add_library(ttl_table STATIC ${TTL_TABLE_SOURCES})
target_include_directories(ttl_table PRIVATE . ..)
