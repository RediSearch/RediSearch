# Build the `arr` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the triemap against the original C implementation.
file(GLOB ARR_SOURCES "arr.c")
add_library(arr STATIC ${ARR_SOURCES})
target_include_directories(arr PRIVATE . ../..)
