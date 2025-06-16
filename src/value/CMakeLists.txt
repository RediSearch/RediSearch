# Build the `RSValue` module as a standalone static library
# This is a temporary requirement to allow us to use the mempool from Rust Bindings
add_library(value STATIC)
target_sources(value PRIVATE "value.c")
target_include_directories(value PRIVATE . ..)
