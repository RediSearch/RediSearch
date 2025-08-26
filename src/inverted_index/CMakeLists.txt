# Build the `inverted_index` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the inverted against the original C implementation.
file(GLOB INVERTED_INDEX_SOURCES "*.c")
add_library(inverted_index STATIC ${INVERTED_INDEX_SOURCES})
target_include_directories(inverted_index PRIVATE . ..)
