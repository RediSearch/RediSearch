# Build the `mempool` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the triemap against the original C implementation.
file(GLOB MEMPOOL_SOURCES "mempool.c")
add_library(mempool STATIC ${MEMPOOL_SOURCES})
target_include_directories(mempool PRIVATE . ../..)
