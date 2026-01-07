# Build the `block_alloc` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB BLOCK_ALLOC_SOURCES "*.c")
add_library(block_alloc STATIC ${BLOCK_ALLOC_SOURCES})
target_include_directories(block_alloc PRIVATE . ../..)
