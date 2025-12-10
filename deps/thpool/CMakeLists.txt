# Build the `thpool` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB THPOOL_SOURCES "*.c")
add_library(thpool STATIC ${THPOOL_SOURCES})
target_include_directories(thpool PRIVATE . ../..)
