# Build the `hll` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB HLL_SOURCES "*.c")
add_library(hll STATIC ${HLL_SOURCES})
target_include_directories(hll PRIVATE . ..)
