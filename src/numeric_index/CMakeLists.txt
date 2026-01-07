# Build the `numeric_index` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB NUMERIC_INDEX_SOURCES "*.c")
add_library(numeric_index STATIC ${NUMERIC_INDEX_SOURCES})
target_include_directories(numeric_index PRIVATE . ..)
