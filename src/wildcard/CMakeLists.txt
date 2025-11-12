# Build the `wildcard` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the triemap against the original C implementation.
file(GLOB ARR_SOURCES "wildcard.c")
add_library(wildcard STATIC ${ARR_SOURCES})
target_include_directories(wildcard PRIVATE . ..)
