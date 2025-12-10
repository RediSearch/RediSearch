# Build the `rlookup` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB RLOOKUP_SOURCES "*.c")
add_library(rlookup STATIC ${RLOOKUP_SOURCES})
target_include_directories(rlookup PRIVATE . ..)
