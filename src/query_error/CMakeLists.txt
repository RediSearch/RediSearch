# Build the `query_error` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the triemap against the original C implementation.
file(GLOB QUERY_ERROR_SOURCES "query_error.c")
add_library(query_error STATIC ${QUERY_ERROR_SOURCES})
target_include_directories(query_error PRIVATE . ../..)
