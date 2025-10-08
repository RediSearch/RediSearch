# Build the `value` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the inverted against the original C implementation.
file(GLOB VALUE_SOURCES "value.c")
add_library(value STATIC ${VALUE_SOURCES})
target_include_directories(value PRIVATE . ..)
