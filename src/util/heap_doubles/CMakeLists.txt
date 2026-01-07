# Build the `heap_doubles` module as a standalone static library.
# This makes it easier for Rust code to call this code by linking to this library.
file(GLOB HEAP_DOUBLES_SOURCES "*.c")
add_library(heap_doubles STATIC ${HEAP_DOUBLES_SOURCES})
target_include_directories(heap_doubles PRIVATE . .. ../..)
