# Build the `mempool` module as a standalone static library
# This is a temporary requirement to allow us to use the mempool from Rust Bindings
file(GLOB ARR_SOURCES "*.c")
add_library(mempool STATIC)
target_sources(mempool PRIVATE ${ARR_SOURCES})
target_include_directories(mempool PRIVATE . ..)
