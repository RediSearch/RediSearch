# Build the `buffer` module as a standalone static library
# This is a temporary requirement to allow us to benchmark the
# Rust implementation of the encoders/decoders against the original C implementation.
file(GLOB BUFFER_SOURCES "buffer.c")
add_library(buffer STATIC ${BUFFER_SOURCES})
target_include_directories(buffer PRIVATE . ..)
