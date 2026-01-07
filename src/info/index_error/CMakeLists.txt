# Build the `index_error` module as a standalone static library.
# This is make it easier for Rust code to call this code by linking to this library.
file(GLOB INDEX_ERROR_SOURCES "*.c")
add_library(index_error STATIC ${INDEX_ERROR_SOURCES})
target_include_directories(index_error PRIVATE . ../..)
