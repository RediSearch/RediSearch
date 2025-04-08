# Build the `triemap` dependency as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the triemap against the original C implementation.
file(GLOB TRIEMAP_SOURCES "triemap.c")
add_library(trie STATIC ${TRIEMAP_SOURCES})
target_include_directories(trie PRIVATE . ../../src)
